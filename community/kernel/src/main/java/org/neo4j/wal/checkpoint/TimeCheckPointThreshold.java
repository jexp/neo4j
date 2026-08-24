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
package org.neo4j.wal.checkpoint;

import static org.neo4j.values.utils.TemporalUtil.NANOS_PER_MILLISECOND;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.neo4j.internal.helpers.Format;
import org.neo4j.time.Stopwatch;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.wal.LogPosition;

class TimeCheckPointThreshold extends AbstractCheckPointThreshold {
    private static final long MAX_MILLIS = Long.MAX_VALUE / NANOS_PER_MILLISECOND;

    private volatile long lastCheckPointedAppendIndex;
    private volatile Duration timeout;
    private volatile Stopwatch stopWatch;

    private final long timeMillisThreshold;
    private final SystemNanoClock clock;

    TimeCheckPointThreshold(long thresholdMillis, SystemNanoClock clock) {
        super("every " + formatDuration(thresholdMillis) + " threshold");
        this.timeMillisThreshold = thresholdMillis;
        this.clock = clock;
        // The random start offset means database in a cluster will not all check-point at the same time.
        this.timeout = randomOffset(thresholdMillis);
        this.stopWatch = clock.startStopWatch();
    }

    @Override
    public void initialize(long appendIndex, LogPosition logPosition) {
        lastCheckPointedAppendIndex = appendIndex;
    }

    @Override
    protected boolean thresholdReached(long lastAppendIndex, LogPosition logPosition) {
        return lastAppendIndex > lastCheckPointedAppendIndex && stopWatch.hasTimedOut(timeout);
    }

    @Override
    public void checkPointHappened(long appendIndex, LogPosition logPosition) {
        lastCheckPointedAppendIndex = appendIndex;
        stopWatch = clock.startStopWatch();
        timeout = Duration.ofMillis(timeMillisThreshold);
    }

    @Override
    public long checkFrequencyMillis() {
        return timeMillisThreshold;
    }

    private static Duration randomOffset(long millis) {
        millis += millis > 0 ? ThreadLocalRandom.current().nextLong(millis) : 0;
        if (millis < 0 || millis > MAX_MILLIS) { // Overflow
            return Duration.ofMillis(MAX_MILLIS);
        }
        return Duration.ofMillis(millis);
    }

    private static String formatDuration(long thresholdMillis) {
        return Format.duration(
                thresholdMillis,
                TimeUnit.DAYS,
                TimeUnit.MILLISECONDS,
                unit -> ' ' + unit.name().toLowerCase());
    }
}
