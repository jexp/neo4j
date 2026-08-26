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
package org.neo4j.kernel.api.impl.index.lucene.v10;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import org.apache.lucene.index.MergePolicy.OneMerge;
import org.apache.lucene.index.MergeScheduler.MergeSource;
import org.apache.lucene.index.MergeTrigger;
import org.junit.jupiter.api.Test;
import org.neo4j.kernel.api.impl.index.lucene.v10.Lucene10Directory.OnThreadConcurrentMergeScheduler;
import org.neo4j.test.OtherThreadExecutor;

/**
 * Compacting a vector index can occupy this scheduler for minutes, so dropping the index or shutting the database down
 * has to be able to cut the merging short.
 */
class OnThreadConcurrentMergeSchedulerTest {
    @Test
    void shouldNotStartMergingOnceAborted() throws Exception {
        OnThreadConcurrentMergeScheduler scheduler = new OnThreadConcurrentMergeScheduler();
        OneMerge first = mock(OneMerge.class);
        OneMerge second = mock(OneMerge.class);
        QueuedMergeSource mergeSource = new QueuedMergeSource(first, second);

        scheduler.abort();
        scheduler.merge(mergeSource, MergeTrigger.EXPLICIT);

        assertThat(mergeSource.merged).isEmpty();
        // Queued merges must still be drained and reported, see shouldReportAbandonedMergesAsFinished
        assertThat(mergeSource.finished).containsExactly(first, second);
    }

    /**
     * {@code forceMerge()} blocks until every merge it queued is accounted for, so a merge we decide not to run has to
     * be reported as finished anyway. Leaving it queued instead deadlocks the thread waiting in {@code forceMerge()}.
     */
    @Test
    void shouldReportAbandonedMergesAsFinished() throws Exception {
        OnThreadConcurrentMergeScheduler scheduler = new OnThreadConcurrentMergeScheduler();
        OneMerge abandoned = mock(OneMerge.class);
        QueuedMergeSource mergeSource = new QueuedMergeSource(abandoned);

        scheduler.abort();
        scheduler.merge(mergeSource, MergeTrigger.EXPLICIT);

        assertThat(mergeSource.hasPendingMerges()).isFalse();
        assertThat(mergeSource.finished).containsExactly(abandoned);
        verify(abandoned).setAborted();
    }

    @Test
    void shouldAbandonRemainingMergesAfterAbort() throws Exception {
        OnThreadConcurrentMergeScheduler scheduler = new OnThreadConcurrentMergeScheduler();
        OneMerge first = mock(OneMerge.class);
        OneMerge second = mock(OneMerge.class);
        QueuedMergeSource mergeSource = new QueuedMergeSource(first, second) {
            @Override
            public void merge(OneMerge merge) {
                super.merge(merge);
                // A real merge notices the abort part-way through; abort while the first one is running
                scheduler.abort();
            }
        };

        scheduler.merge(mergeSource, MergeTrigger.EXPLICIT);

        assertThat(mergeSource.merged).containsExactly(first);
        assertThat(mergeSource.finished).containsExactly(second);
        verify(second).setAborted();
    }

    @Test
    void shouldAbortTheMergeThatIsAlreadyRunning() throws Exception {
        OnThreadConcurrentMergeScheduler scheduler = new OnThreadConcurrentMergeScheduler();
        OneMerge running = mock(OneMerge.class);

        CountDownLatch mergeStarted = new CountDownLatch(1);
        CountDownLatch releaseMerge = new CountDownLatch(1);
        QueuedMergeSource mergeSource = new QueuedMergeSource(running) {
            @Override
            public void merge(OneMerge merge) {
                super.merge(merge);
                mergeStarted.countDown();
                try {
                    assertTrue(releaseMerge.await(10, SECONDS));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        };

        try (OtherThreadExecutor mergingThread = new OtherThreadExecutor("Merging thread")) {
            Future<Void> merging = mergingThread.executeDontWait(() -> {
                scheduler.merge(mergeSource, MergeTrigger.EXPLICIT);
                return null;
            });
            assertTrue(mergeStarted.await(10, SECONDS));

            scheduler.abort();

            // The in-flight merge is told to give up, which is what makes Lucene unwind it early
            verify(running).setAborted();

            releaseMerge.countDown();
            merging.get(10, SECONDS);
        }
    }

    private static class QueuedMergeSource implements MergeSource {
        private final Deque<OneMerge> pending = new ArrayDeque<>();
        private final List<OneMerge> merged = new ArrayList<>();
        private final List<OneMerge> finished = new ArrayList<>();

        QueuedMergeSource(OneMerge... merges) {
            pending.addAll(List.of(merges));
        }

        @Override
        public OneMerge getNextMerge() {
            return pending.poll();
        }

        @Override
        public void onMergeFinished(OneMerge merge) {
            finished.add(merge);
        }

        @Override
        public boolean hasPendingMerges() {
            return !pending.isEmpty();
        }

        @Override
        public void merge(OneMerge merge) {
            merged.add(merge);
        }
    }
}
