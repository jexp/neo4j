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
package org.neo4j.internal.collector;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.graph_stats_collection_divergence_threshold;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.graph_stats_collection_enabled;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.graph_stats_collection_interval;
import static org.neo4j.token.api.TokenConstants.ANY_LABEL;
import static org.neo4j.token.api.TokenConstants.ANY_RELATIONSHIP_TYPE;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.InternalLog;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.token.api.NamedToken;

class GraphStatsCollectorTest {

    private static final String SCOPE = "test";

    @Test
    void shouldNotScheduleAJobWhenDisabled() {
        Config config = Config.defaults();
        JobScheduler scheduler = mock(JobScheduler.class);
        GraphStatsCollector collector = newCollector(config, scheduler, mock(Database.class), mockSink());

        collector.start();

        verifyNoInteractions(scheduler);
    }

    @Test
    void shouldScheduleAJobWhenEnabled() {
        Config config = Config.defaults();
        config.setDynamic(graph_stats_collection_enabled, true, SCOPE);
        config.setDynamic(graph_stats_collection_interval, Duration.ofMinutes(5), SCOPE);
        JobScheduler scheduler = mock(JobScheduler.class);
        stubSchedule(scheduler, mockJobHandle());

        GraphStatsCollector collector = newCollector(config, scheduler, mock(Database.class), mockSink());
        collector.start();

        verify(scheduler)
                .scheduleRecurring(
                        eq(Group.DATA_COLLECTOR),
                        any(Runnable.class),
                        eq(0L),
                        eq(Duration.ofMinutes(5).toMillis()),
                        eq(MILLISECONDS));
    }

    @Test
    void shouldStartScheduledJobWhenEnabledDynamically() {
        Config config = Config.defaults();
        JobScheduler scheduler = mock(JobScheduler.class);
        stubSchedule(scheduler, mockJobHandle());

        GraphStatsCollector collector = newCollector(config, scheduler, mock(Database.class), mockSink());
        collector.start();
        verifyNoInteractions(scheduler);

        config.setDynamic(graph_stats_collection_enabled, true, SCOPE);

        verify(scheduler)
                .scheduleRecurring(
                        eq(Group.DATA_COLLECTOR), any(Runnable.class), anyLong(), anyLong(), eq(MILLISECONDS));
    }

    @Test
    void shouldCancelScheduledJobWhenDisabledDynamically() {
        Config config = Config.defaults();
        config.setDynamic(graph_stats_collection_enabled, true, SCOPE);
        JobScheduler scheduler = mock(JobScheduler.class);
        JobHandle<Object> handle = mockJobHandle();
        stubSchedule(scheduler, handle);

        GraphStatsCollector collector = newCollector(config, scheduler, mock(Database.class), mockSink());
        collector.start();

        config.setDynamic(graph_stats_collection_enabled, false, SCOPE);

        verify(handle).cancel();
    }

    @Test
    void shouldRescheduleWhenDurationChangesDynamically() {
        Config config = Config.defaults();
        config.setDynamic(graph_stats_collection_enabled, true, SCOPE);
        JobScheduler scheduler = mock(JobScheduler.class);
        JobHandle<Object> firstHandle = mockJobHandle();
        JobHandle<Object> secondHandle = mockJobHandle();
        doReturn(firstHandle, secondHandle)
                .when(scheduler)
                .scheduleRecurring(any(), any(Runnable.class), anyLong(), anyLong(), eq(MILLISECONDS));

        GraphStatsCollector collector = newCollector(config, scheduler, mock(Database.class), mockSink());
        collector.start();

        config.setDynamic(graph_stats_collection_interval, Duration.ofMinutes(1), SCOPE);

        verify(firstHandle).cancel();
        verify(scheduler, times(2))
                .scheduleRecurring(
                        eq(Group.DATA_COLLECTOR), any(Runnable.class), anyLong(), anyLong(), eq(MILLISECONDS));
    }

    @Test
    void stopShouldCancelTheScheduledJobAndStopReactingToSettingChanges() {
        Config config = Config.defaults();
        config.setDynamic(graph_stats_collection_enabled, true, SCOPE);
        JobScheduler scheduler = mock(JobScheduler.class);
        JobHandle<Object> handle = mockJobHandle();
        stubSchedule(scheduler, handle);

        GraphStatsCollector collector = newCollector(config, scheduler, mock(Database.class), mockSink());
        collector.start();
        collector.stop();

        verify(handle).cancel();

        config.setDynamic(graph_stats_collection_enabled, false, SCOPE);
        config.setDynamic(graph_stats_collection_enabled, true, SCOPE);

        verify(scheduler, times(1))
                .scheduleRecurring(
                        eq(Group.DATA_COLLECTOR), any(Runnable.class), anyLong(), anyLong(), eq(MILLISECONDS));
    }

    @Test
    @SuppressWarnings("unchecked")
    void collectShouldRetrieveGraphCountsAndLogThem() throws Exception {
        Config config = Config.defaults();
        config.setDynamic(graph_stats_collection_enabled, true, SCOPE);
        JobScheduler scheduler = mock(JobScheduler.class);
        ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        doReturn(mockJobHandle())
                .when(scheduler)
                .scheduleRecurring(any(), runnableCaptor.capture(), anyLong(), anyLong(), eq(MILLISECONDS));

        Database db = mock(Database.class);
        GraphDatabaseAPI databaseAPI = mock(GraphDatabaseAPI.class);
        when(db.getDatabaseAPI()).thenReturn(databaseAPI);
        when(databaseAPI.databaseName()).thenReturn("neo4j");

        Kernel kernel = mock(Kernel.class);
        KernelTransaction tx = mock(KernelTransaction.class);
        TokenRead tokenRead = mock(TokenRead.class);
        Read dataRead = mock(Read.class);
        SchemaRead schemaRead = mock(SchemaRead.class);
        when(db.getKernel()).thenReturn(kernel);
        when(kernel.beginTransaction(KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED))
                .thenReturn(tx);
        when(tx.tokenRead()).thenReturn(tokenRead);
        when(tx.dataRead()).thenReturn(dataRead);
        when(tx.schemaRead()).thenReturn(schemaRead);
        when(tokenRead.labelsGetAllTokens()).thenReturn(Collections.emptyIterator());
        when(tokenRead.relationshipTypesGetAllTokens()).thenReturn(Collections.emptyIterator());
        when(schemaRead.indexesGetAll()).thenReturn(Collections.emptyIterator());
        when(schemaRead.constraintsGetAll()).thenReturn(Collections.emptyIterator());
        when(dataRead.estimateCountsForNode(ANY_LABEL)).thenReturn(42L);
        when(dataRead.estimateCountsForRelationships(ANY_LABEL, ANY_RELATIONSHIP_TYPE, ANY_LABEL))
                .thenReturn(7L);

        BiConsumer<String, Map<String, Object>> sink = mockSink();
        GraphStatsCollector collector = newCollector(config, scheduler, db, sink);
        collector.start();

        runnableCaptor.getValue().run();

        ArgumentCaptor<Map<String, Object>> graphCountsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(sink).accept(eq("neo4j"), graphCountsCaptor.capture());
        Map<String, Object> graphCounts = graphCountsCaptor.getValue();
        assertThat(graphCounts)
                .containsEntry("nodes", List.of(Map.of("count", 42L)))
                .containsEntry("relationships", List.of(Map.of("count", 7L)))
                .containsEntry("indexes", List.of())
                .containsEntry("constraints", List.of());
    }

    @Test
    void collectShouldNotPropagateFailures() {
        Config config = Config.defaults();
        config.setDynamic(graph_stats_collection_enabled, true, SCOPE);
        JobScheduler scheduler = mock(JobScheduler.class);
        ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        doReturn(mockJobHandle())
                .when(scheduler)
                .scheduleRecurring(any(), runnableCaptor.capture(), anyLong(), anyLong(), eq(MILLISECONDS));

        Database db = mock(Database.class);
        GraphDatabaseAPI databaseAPI = mock(GraphDatabaseAPI.class);
        when(db.getDatabaseAPI()).thenReturn(databaseAPI);
        when(databaseAPI.databaseName()).thenReturn("neo4j");
        when(db.getKernel()).thenThrow(new RuntimeException("boom"));

        BiConsumer<String, Map<String, Object>> sink = mockSink();
        InternalLog log = mock(InternalLog.class);
        GraphStatsCollector collector = new GraphStatsCollector(config, db, scheduler, sink, log);
        collector.start();

        runnableCaptor.getValue().run();

        verify(sink, never()).accept(anyString(), anyMap());
        verify(log).warn(anyString(), any(Exception.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void secondTickBelowThresholdDoesNotLogAndRetainsFirstSnapshot() throws Exception {
        Config config = Config.defaults();
        config.setDynamic(graph_stats_collection_enabled, true, SCOPE);
        config.setDynamic(graph_stats_collection_divergence_threshold, 0.5, SCOPE);
        MockedKernel kernel = mockKernel();

        BiConsumer<String, Map<String, Object>> sink = mockSink();
        Runnable collect = startCollectorAndCaptureCollect(config, kernel.db(), sink);

        stubGraphCounts(kernel, 100L, Map.of("Person", 100L), List.of(), List.of());
        collect.run();

        stubGraphCounts(kernel, 130L, Map.of("Person", 130L), List.of(), List.of());
        collect.run();

        // Divergence from the retained (first) snapshot, not from the skipped second tick: if the
        // second tick had wrongly become retained, this divergence would fall back under the threshold.
        stubGraphCounts(kernel, 250L, Map.of("Person", 250L), List.of(), List.of());
        collect.run();

        ArgumentCaptor<Map<String, Object>> graphCountsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(sink, times(2)).accept(eq("neo4j"), graphCountsCaptor.capture());
        assertThat(graphCountsCaptor.getAllValues().get(0))
                .containsEntry("nodes", List.of(Map.of("count", 100L), Map.of("count", 100L, "label", "Person")));
        assertThat(graphCountsCaptor.getAllValues().get(1))
                .containsEntry("nodes", List.of(Map.of("count", 250L), Map.of("count", 250L, "label", "Person")));
    }

    @Test
    @SuppressWarnings("unchecked")
    void secondTickAboveThresholdLogsAndUpdatesRetainedSnapshot() throws Exception {
        Config config = Config.defaults();
        config.setDynamic(graph_stats_collection_enabled, true, SCOPE);
        config.setDynamic(graph_stats_collection_divergence_threshold, 0.5, SCOPE);
        MockedKernel kernel = mockKernel();

        BiConsumer<String, Map<String, Object>> sink = mockSink();
        Runnable collect = startCollectorAndCaptureCollect(config, kernel.db(), sink);

        stubGraphCounts(kernel, 100L, Map.of("Person", 100L), List.of(), List.of());
        collect.run();

        stubGraphCounts(kernel, 400L, Map.of("Person", 400L), List.of(), List.of());
        collect.run();

        ArgumentCaptor<Map<String, Object>> graphCountsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(sink, times(2)).accept(eq("neo4j"), graphCountsCaptor.capture());
        assertThat(graphCountsCaptor.getAllValues().get(1))
                .containsEntry("nodes", List.of(Map.of("count", 400L), Map.of("count", 400L, "label", "Person")));
    }

    @Test
    @SuppressWarnings("unchecked")
    void labelSetChangeAlwaysLogsRegardlessOfDivergence() throws Exception {
        Config config = Config.defaults();
        config.setDynamic(graph_stats_collection_enabled, true, SCOPE);
        MockedKernel kernel = mockKernel();

        BiConsumer<String, Map<String, Object>> sink = mockSink();
        Runnable collect = startCollectorAndCaptureCollect(config, kernel.db(), sink);

        stubGraphCounts(kernel, 100L, Map.of("Person", 100L), List.of(), List.of());
        collect.run();

        stubGraphCounts(kernel, 100L, Map.of("Person", 100L, "Movie", 5L), List.of(), List.of());
        collect.run();

        ArgumentCaptor<Map<String, Object>> graphCountsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(sink, times(2)).accept(eq("neo4j"), graphCountsCaptor.capture());
        assertThat(graphCountsCaptor.getAllValues().get(1))
                .extracting("nodes")
                .asList()
                .contains(Map.of("count", 5L, "label", "Movie"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void indexAddedAlwaysLogsRegardlessOfDivergence() throws Exception {
        Config config = Config.defaults();
        config.setDynamic(graph_stats_collection_enabled, true, SCOPE);
        MockedKernel kernel = mockKernel();

        BiConsumer<String, Map<String, Object>> sink = mockSink();
        Runnable collect = startCollectorAndCaptureCollect(config, kernel.db(), sink);

        stubGraphCounts(kernel, 100L, Map.of("Person", 100L), List.of(), List.of());
        collect.run();

        IndexDescriptor index = IndexPrototype.forSchema(SchemaDescriptors.forLabel(0, 0))
                .withName("person_index")
                .materialise(1);
        stubGraphCounts(kernel, 100L, Map.of("Person", 100L), List.of(index), List.of());
        collect.run();

        ArgumentCaptor<Map<String, Object>> graphCountsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(sink, times(2)).accept(eq("neo4j"), graphCountsCaptor.capture());
        assertThat(graphCountsCaptor.getAllValues().get(1))
                .extracting("indexes")
                .asList()
                .hasSize(1);
    }

    @Test
    @SuppressWarnings("unchecked")
    void constraintAddedAlwaysLogsRegardlessOfDivergence() throws Exception {
        Config config = Config.defaults();
        config.setDynamic(graph_stats_collection_enabled, true, SCOPE);
        MockedKernel kernel = mockKernel();

        BiConsumer<String, Map<String, Object>> sink = mockSink();
        Runnable collect = startCollectorAndCaptureCollect(config, kernel.db(), sink);

        stubGraphCounts(kernel, 100L, Map.of("Person", 100L), List.of(), List.of());
        collect.run();

        ConstraintDescriptor constraint = ConstraintDescriptorFactory.uniqueForSchema(SchemaDescriptors.forLabel(0, 0));
        stubGraphCounts(kernel, 100L, Map.of("Person", 100L), List.of(), List.of(constraint));
        collect.run();

        ArgumentCaptor<Map<String, Object>> graphCountsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(sink, times(2)).accept(eq("neo4j"), graphCountsCaptor.capture());
        assertThat(graphCountsCaptor.getAllValues().get(1))
                .extracting("constraints")
                .asList()
                .hasSize(1);
    }

    @Test
    @SuppressWarnings("unchecked")
    void thresholdOfZeroLogsOnEveryChangingTick() throws Exception {
        Config config = Config.defaults();
        config.setDynamic(graph_stats_collection_enabled, true, SCOPE);
        config.setDynamic(graph_stats_collection_divergence_threshold, 0.0, SCOPE);
        MockedKernel kernel = mockKernel();

        BiConsumer<String, Map<String, Object>> sink = mockSink();
        Runnable collect = startCollectorAndCaptureCollect(config, kernel.db(), sink);

        stubGraphCounts(kernel, 100L, Map.of("Person", 100L), List.of(), List.of());
        collect.run();

        // A threshold of 0.0 reproduces "log every tick" for any actual change, however small;
        // an exactly-unchanged tick has zero divergence, which is not > 0.0 and so is not stale.
        stubGraphCounts(kernel, 101L, Map.of("Person", 101L), List.of(), List.of());
        collect.run();

        stubGraphCounts(kernel, 102L, Map.of("Person", 102L), List.of(), List.of());
        collect.run();

        verify(sink, times(3)).accept(eq("neo4j"), anyMap());
    }

    private record MockedKernel(Database db, TokenRead tokenRead, Read dataRead, SchemaRead schemaRead) {}

    @SuppressWarnings("unchecked")
    private static MockedKernel mockKernel() throws Exception {
        Database db = mock(Database.class);
        GraphDatabaseAPI databaseAPI = mock(GraphDatabaseAPI.class);
        when(db.getDatabaseAPI()).thenReturn(databaseAPI);
        when(databaseAPI.databaseName()).thenReturn("neo4j");

        Kernel kernel = mock(Kernel.class);
        KernelTransaction tx = mock(KernelTransaction.class);
        TokenRead tokenRead = mock(TokenRead.class);
        Read dataRead = mock(Read.class);
        SchemaRead schemaRead = mock(SchemaRead.class);
        when(db.getKernel()).thenReturn(kernel);
        when(kernel.beginTransaction(KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED))
                .thenReturn(tx);
        when(tx.tokenRead()).thenReturn(tokenRead);
        when(tx.dataRead()).thenReturn(dataRead);
        when(tx.schemaRead()).thenReturn(schemaRead);

        return new MockedKernel(db, tokenRead, dataRead, schemaRead);
    }

    private static Runnable startCollectorAndCaptureCollect(
            Config config, Database db, BiConsumer<String, Map<String, Object>> sink) {
        JobScheduler scheduler = mock(JobScheduler.class);
        ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        doReturn(mockJobHandle())
                .when(scheduler)
                .scheduleRecurring(any(), runnableCaptor.capture(), anyLong(), anyLong(), eq(MILLISECONDS));

        GraphStatsCollector collector = newCollector(config, scheduler, db, sink);
        collector.start();

        return runnableCaptor.getValue();
    }

    private static void stubGraphCounts(
            MockedKernel kernel,
            long totalNodeCount,
            Map<String, Long> labelCounts,
            List<IndexDescriptor> indexes,
            List<ConstraintDescriptor> constraints)
            throws Exception {
        TokenRead tokenRead = kernel.tokenRead();
        Read dataRead = kernel.dataRead();
        SchemaRead schemaRead = kernel.schemaRead();

        when(dataRead.estimateCountsForNode(ANY_LABEL)).thenReturn(totalNodeCount);
        when(dataRead.estimateCountsForRelationships(ANY_LABEL, ANY_RELATIONSHIP_TYPE, ANY_LABEL))
                .thenReturn(0L);

        List<NamedToken> labelTokens = new ArrayList<>();
        int labelId = 0;
        for (Map.Entry<String, Long> entry : labelCounts.entrySet()) {
            int id = labelId++;
            labelTokens.add(new NamedToken(entry.getKey(), id));
            when(dataRead.estimateCountsForNode(id)).thenReturn(entry.getValue());
        }
        when(tokenRead.labelsGetAllTokens()).thenReturn(labelTokens.iterator());
        when(tokenRead.relationshipTypesGetAllTokens()).thenReturn(Collections.emptyIterator());
        when(tokenRead.labelGetName(anyInt())).thenReturn("Label");
        when(tokenRead.propertyKeyGetName(anyInt())).thenReturn("prop");

        when(schemaRead.indexesGetAll()).thenReturn(indexes.iterator());
        when(schemaRead.constraintsGetAll()).thenReturn(constraints.iterator());
        for (IndexDescriptor index : indexes) {
            when(schemaRead.indexSample(index)).thenReturn(new IndexSample());
        }
    }

    private static GraphStatsCollector newCollector(
            Config config, JobScheduler scheduler, Database db, BiConsumer<String, Map<String, Object>> sink) {
        return new GraphStatsCollector(config, db, scheduler, sink, mock(InternalLog.class));
    }

    @SuppressWarnings("unchecked")
    private static BiConsumer<String, Map<String, Object>> mockSink() {
        return mock(BiConsumer.class);
    }

    private static void stubSchedule(JobScheduler scheduler, JobHandle<?> handle) {
        doReturn(handle)
                .when(scheduler)
                .scheduleRecurring(any(), any(Runnable.class), anyLong(), anyLong(), eq(MILLISECONDS));
    }

    @SuppressWarnings("unchecked")
    private static JobHandle<Object> mockJobHandle() {
        return mock(JobHandle.class);
    }
}
