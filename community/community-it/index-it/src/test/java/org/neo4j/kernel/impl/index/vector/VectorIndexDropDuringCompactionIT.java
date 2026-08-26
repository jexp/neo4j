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
import static org.neo4j.logging.AssertableLogProvider.Level.ERROR;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.kernel.api.IndexMonitor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.api.impl.index.lucene.LuceneSettings;
import org.neo4j.kernel.api.impl.index.lucene.LuceneSettings.PostPopulationCompaction;
import org.neo4j.kernel.api.schema.vector.VectorTestUtils.VectorIndexSettings;
import org.neo4j.kernel.api.vector.VectorSimilarityFunction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogAssertions;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;

/**
 * Dropping a vector index while its post-population compaction is merging segments used to queue behind the merge,
 * because compaction ran inside the index flip and so held the flip lock for its whole duration. Since a merge of a
 * large index can take hours, that made the drop appear to hang.
 */
@Neo4jLayoutExtension
class VectorIndexDropDuringCompactionIT {
    private static final Label LABEL = Label.label("Vector");
    private static final String PROPERTY = "embedding";
    private static final String INDEX_NAME = "vectorIndex";
    private static final VectorSimilarityFunction SIMILARITY_FUNCTION =
            LatestVersions.LATEST_VECTOR_INDEX_VERSION.similarityFunction("COSINE");

    // Enough vectors that the forced merge takes seconds, so that a drop issued the moment compaction starts
    // reliably lands while a Lucene merge is genuinely in flight.
    private static final int NODES = 8_000;
    private static final int DIMENSIONS = 128;

    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private DatabaseLayout databaseLayout;

    private final AssertableLogProvider logProvider = new AssertableLogProvider();
    private DatabaseManagementService managementService;
    private GraphDatabaseAPI db;

    @AfterEach
    void tearDown() {
        if (managementService != null) {
            managementService.shutdown();
        }
    }

    /**
     * Uses FULL rather than the default AUTO deliberately. AUTO's {@code maybeMerge()} finishes too quickly at any
     * dataset size that keeps this test non-flaky.
     */
    @Test
    void shouldDropVectorIndexWhileItIsCompacting() throws Exception {
        // GIVEN a populated store and compaction configured to do a substantial merge
        startDb();
        createVectorNodes();

        CountDownLatch compactionStarted = new CountDownLatch(1);
        CountDownLatch compactionAborted = new CountDownLatch(1);
        AtomicLong compactingIndexId = new AtomicLong(-1);
        db.getDependencyResolver()
                .resolveDependency(Monitors.class)
                .addMonitorListener(new IndexMonitor.MonitorAdapter() {
                    @Override
                    public void postPopulationCompactionStarted(IndexDescriptor descriptor) {
                        compactingIndexId.set(descriptor.getId());
                        compactionStarted.countDown();
                    }

                    @Override
                    public void postPopulationCompactionAborted(IndexDescriptor descriptor) {
                        compactionAborted.countDown();
                    }
                });

        // and a population that has reached its compaction phase
        createVectorIndex(INDEX_NAME);
        assertTrue(compactionStarted.await(5, MINUTES), "post-population compaction never started");

        // WHEN dropping the index while that merge is running
        dropIndex(INDEX_NAME);

        // THEN the drop reached the merge and cut it short, rather than queueing behind it.
        // This signal is only reported when a compaction really was in flight, so it also establishes the timing.
        assertTrue(
                compactionAborted.await(1, MINUTES),
                "the in-flight compaction was never asked to abandon its merge, so the drop waited the merge out "
                        + "instead of cutting it short");

        // and the index is gone, with no files left behind
        assertIndexGone(INDEX_NAME);
        assertNoIndexFilesExisting(compactingIndexId.get());

        // and aborting the merge did not break anything
        LogAssertions.assertThat(logProvider).forLevel(ERROR).doesNotHaveAnyLogs();
        assertDatabaseStillUsable();
    }

    @Test
    void shouldDropVectorIndexWhileItIsStillScanning() throws Exception {
        startDb();
        createVectorNodes();

        CountDownLatch compactionStarted = new CountDownLatch(1);
        db.getDependencyResolver()
                .resolveDependency(Monitors.class)
                .addMonitorListener(new IndexMonitor.MonitorAdapter() {
                    @Override
                    public void postPopulationCompactionStarted(IndexDescriptor descriptor) {
                        compactionStarted.countDown();
                    }
                });

        createVectorIndex(INDEX_NAME);

        // WHEN dropping while the scan is still running, well before the compaction phase
        long start = System.nanoTime();
        dropIndex(INDEX_NAME);
        Duration dropTook = Duration.ofNanos(System.nanoTime() - start);

        // THEN
        assertThat(compactionStarted.getCount())
                .as("this exercises the scan phase, so compaction should never have started")
                .isEqualTo(1);
        assertThat(dropTook)
                .as("drop should not wait for an in-flight population merge")
                .isLessThan(Duration.ofSeconds(30));

        assertIndexGone(INDEX_NAME);
        LogAssertions.assertThat(logProvider).forLevel(ERROR).doesNotHaveAnyLogs();
        assertDatabaseStillUsable();
    }

    private void startDb() {
        managementService = new TestDatabaseManagementServiceBuilder(databaseLayout)
                .setInternalLogProvider(logProvider)
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
     * background while the test drops it.
     */
    private void createVectorIndex(String name) {
        try (Transaction tx = db.beginTx()) {
            VectorIndexSettings settings =
                    VectorIndexSettings.create().withDimensions(DIMENSIONS).withSimilarityFunction(SIMILARITY_FUNCTION);
            tx.schema()
                    .indexFor(LABEL)
                    .on(PROPERTY)
                    .withIndexType(IndexType.VECTOR.toPublicApi())
                    .withIndexConfiguration(settings.toMap())
                    .withName(name)
                    .create();
            tx.commit();
        }
    }

    private void dropIndex(String name) {
        try (Transaction tx = db.beginTx()) {
            tx.schema().getIndexByName(name).drop();
            tx.commit();
        }
    }

    private void assertIndexGone(String name) {
        try (Transaction tx = db.beginTx()) {
            assertThat(tx.schema().getIndexes())
                    .as("dropped index should no longer be listed")
                    .noneSatisfy(index -> assertThat(index.getName()).isEqualTo(name));
        }
    }

    /**
     * Scoped to the dropped index rather than the whole schema/index tree, since the default token indexes live there
     * too and are deliberately left alone.
     */
    private void assertNoIndexFilesExisting(long indexId) throws IOException {
        Path indexDir =
                db.databaseLayout().databaseDirectory().resolve("schema").resolve("index");
        for (Path providerDirectory : fs.listFiles(indexDir)) {
            assertThat(providerDirectory.resolve(String.valueOf(indexId)))
                    .as("index files left behind after dropping mid-merge")
                    .doesNotExist();
        }
    }

    private void assertDatabaseStillUsable() {
        try (Transaction tx = db.beginTx()) {
            assertThat(tx.execute("RETURN 1 AS one").<Long>columnAs("one").next())
                    .isEqualTo(1L);
        }

        // A fresh index over the same data must populate, compact and come online as usual
        createVectorIndex("recreated");
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexOnline("recreated", 5, MINUTES);
            IndexDefinition recreated = tx.schema().getIndexByName("recreated");
            assertThat(tx.schema().getIndexState(recreated))
                    .isEqualTo(org.neo4j.graphdb.schema.Schema.IndexState.ONLINE);
        }
    }
}
