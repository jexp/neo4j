/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.index.vector;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.IndexMonitor;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.api.impl.index.lucene.LuceneSettings;
import org.neo4j.kernel.api.impl.index.lucene.LuceneSettings.PostPopulationCompaction;
import org.neo4j.kernel.api.schema.vector.VectorTestUtils.VectorIndexSettings;
import org.neo4j.kernel.api.vector.VectorSimilarityFunction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;

/**
 * Shutting the database down while a vector index is running its post-population compaction must not wait for that
 * merge to finish. Abandoning the merge means the population as a whole is abandoned, so the index has to come back
 * as {@link InternalIndexState#POPULATING} and be rebuilt - marking it online with the segments it happens to have
 * merged so far would leave a permanently uncompacted index that nothing would ever go back and fix.
 */
@Neo4jLayoutExtension
class VectorIndexShutdownDuringCompactionIT {
    private static final Label LABEL = Label.label("Vector");
    private static final String PROPERTY = "embedding";
    private static final String INDEX_NAME = "vectorIndex";
    private static final VectorSimilarityFunction SIMILARITY_FUNCTION =
            LatestVersions.LATEST_VECTOR_INDEX_VERSION.similarityFunction("COSINE");

    // Enough vectors that the forced merge takes seconds, so the shutdown reliably lands mid-merge
    private static final int NODES = 8_000;
    private static final int DIMENSIONS = 128;

    @Inject
    private DatabaseLayout databaseLayout;

    private DatabaseManagementService managementService;
    private GraphDatabaseAPI db;

    @AfterEach
    void tearDown() {
        if (managementService != null) {
            managementService.shutdown();
        }
    }

    @Test
    void shouldNotWaitForCompactionOnShutdownAndShouldRepopulateAfterwards() throws Exception {
        // GIVEN a population that has reached its compaction phase
        CountDownLatch compactionStarted = new CountDownLatch(1);
        CountDownLatch compactionAborted = new CountDownLatch(1);
        Monitors monitors = new Monitors();
        monitors.addMonitorListener(new IndexMonitor.MonitorAdapter() {
            @Override
            public void postPopulationCompactionStarted(IndexDescriptor descriptor) {
                compactionStarted.countDown();
            }

            @Override
            public void postPopulationCompactionAborted(IndexDescriptor descriptor) {
                compactionAborted.countDown();
            }
        });

        startDb(monitors);
        createVectorNodes();
        createVectorIndex();
        assertTrue(compactionStarted.await(5, MINUTES), "post-population compaction never started");

        // WHEN shutting down while that merge is running
        managementService.shutdown();
        managementService = null;

        // THEN the shutdown cut the merge short rather than waiting it out.
        // This signal is only reported when a compaction really was in flight, so it also establishes the timing.
        assertTrue(
                compactionAborted.await(1, MINUTES),
                "the in-flight compaction was never asked to abandon its merge, so the shutdown waited the merge out");

        // and the abandoned population is not passed off as a finished index: it comes back to be rebuilt rather
        // than online with whatever segments the merge happened to have produced
        Map<String, InternalIndexState> statesOnRestart = new ConcurrentHashMap<>();
        Monitors restartMonitors = new Monitors();
        restartMonitors.addMonitorListener(new IndexMonitor.MonitorAdapter() {
            @Override
            public void initialState(String databaseName, IndexDescriptor descriptor, InternalIndexState state) {
                statesOnRestart.put(descriptor.getName(), state);
            }
        });
        startDb(restartMonitors);

        assertThat(statesOnRestart)
                .as("index state read from disk on restart")
                .containsEntry(INDEX_NAME, InternalIndexState.POPULATING);

        // and the rebuild completes as usual
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexOnline(INDEX_NAME, 5, MINUTES);
        }
    }

    private void startDb(Monitors monitors) {
        managementService = new TestDatabaseManagementServiceBuilder(databaseLayout)
                .setMonitors(monitors)
                .setConfig(LuceneSettings.vector_post_population_compaction, PostPopulationCompaction.FULL)
                .build();
        db = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
    }

    private void createVectorNodes() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int batchSize = 1_000;
        for (int batch = 0; batch < NODES / batchSize; batch++) {
            try (Transaction tx = db.beginTx()) {
                for (int i = 0; i < batchSize; i++) {
                    float[] vector = new float[DIMENSIONS];
                    for (int d = 0; d < DIMENSIONS; d++) {
                        vector[d] = random.nextFloat();
                    }
                    Node node = tx.createNode(LABEL);
                    node.setProperty(PROPERTY, vector);
                }
                tx.commit();
            }
        }
    }

    /**
     * Creates the index without awaiting it coming online, so that population - and then compaction - runs in the
     * background while the test shuts the database down.
     */
    private void createVectorIndex() {
        try (Transaction tx = db.beginTx()) {
            VectorIndexSettings settings =
                    VectorIndexSettings.create().withDimensions(DIMENSIONS).withSimilarityFunction(SIMILARITY_FUNCTION);
            tx.schema()
                    .indexFor(LABEL)
                    .on(PROPERTY)
                    .withIndexType(IndexType.VECTOR.toPublicApi())
                    .withIndexConfiguration(settings.toMap())
                    .withName(INDEX_NAME)
                    .create();
            tx.commit();
        }
    }
}
