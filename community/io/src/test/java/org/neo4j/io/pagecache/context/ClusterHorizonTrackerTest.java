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
package org.neo4j.io.pagecache.context;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.neo4j.io.pagecache.context.ClusterHorizonTracker.ClusterHorizonTrackerImpl;

class ClusterHorizonTrackerTest {
    private final ClusterHorizonTracker tracker = new ClusterHorizonTrackerImpl();

    @Test
    void noPinsPausesCleanup() {
        assertThat(tracker.oldestVisibilityHorizon()).isEqualTo(Long.MIN_VALUE);

        tracker.unpin(tracker.pin(7));

        assertThat(tracker.oldestVisibilityHorizon()).isEqualTo(Long.MIN_VALUE);
    }

    @Test
    void singlePinExposesItsHorizon() {
        tracker.pin(42);

        assertThat(tracker.oldestVisibilityHorizon()).isEqualTo(42);
    }

    @Test
    void oldestOfSeveralPinsIsExposedRegardlessOfPinOrder() {
        tracker.pin(17);
        tracker.pin(5);
        tracker.pin(23);

        assertThat(tracker.oldestVisibilityHorizon()).isEqualTo(5);
    }

    @Test
    void horizonMovesForwardOnlyWhenOldestPinIsReleased() {
        var oldest = tracker.pin(5);
        var middle = tracker.pin(17);
        tracker.pin(23);

        tracker.unpin(middle);
        assertThat(tracker.oldestVisibilityHorizon()).isEqualTo(5);

        tracker.unpin(oldest);
        assertThat(tracker.oldestVisibilityHorizon()).isEqualTo(23);
    }

    @Test
    void pinsWithEqualHorizonsAreTrackedIndividually() {
        var first = tracker.pin(5);
        tracker.pin(5);

        tracker.unpin(first);

        assertThat(tracker.oldestVisibilityHorizon()).isEqualTo(5);
    }

    @Test
    void unpinningTwiceDoesNotReleaseOtherPins() {
        var pin = tracker.pin(5);
        tracker.pin(17);

        tracker.unpin(pin);
        tracker.unpin(pin);

        assertThat(tracker.oldestVisibilityHorizon()).isEqualTo(17);
    }

    @Test
    void noOpTrackerNeverConstrainsCleanup() {
        var noOp = ClusterHorizonTracker.NO_OP;

        assertThat(noOp.oldestVisibilityHorizon()).isEqualTo(Long.MAX_VALUE);

        var pin = noOp.pin(5);
        assertThat(pin).isNull();
        assertThat(noOp.oldestVisibilityHorizon()).isEqualTo(Long.MAX_VALUE);

        noOp.unpin(pin);
        assertThat(noOp.oldestVisibilityHorizon()).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void concurrentPinningNeverExposesHorizonYoungerThanTheOldestLivePin() throws Exception {
        int threads = 8;
        int pinsPerThread = 500;
        // the lowest horizon any thread will pin, kept pinned for the whole run
        var barrier = tracker.pin(0);
        var startLatch = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                int base = i * pinsPerThread + 1;
                futures.add(executor.submit(() -> {
                    startLatch.await();
                    for (int pinIndex = 0; pinIndex < pinsPerThread; pinIndex++) {
                        var pin = tracker.pin(base + pinIndex);
                        assertThat(tracker.oldestVisibilityHorizon()).isEqualTo(0);
                        tracker.unpin(pin);
                    }
                    return null;
                }));
            }
            startLatch.countDown();
            for (var future : futures) {
                future.get(1, TimeUnit.MINUTES);
            }
        } finally {
            executor.shutdownNow();
        }

        assertThat(tracker.oldestVisibilityHorizon()).isEqualTo(0);

        tracker.unpin(barrier);
        assertThat(tracker.oldestVisibilityHorizon()).isEqualTo(Long.MIN_VALUE);
    }
}
