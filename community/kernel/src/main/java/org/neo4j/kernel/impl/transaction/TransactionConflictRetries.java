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
package org.neo4j.kernel.impl.transaction;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.time.Duration;
import org.neo4j.storageengine.api.txstate.validation.TransactionConflictException;
import org.neo4j.time.SystemNanoClock;

public final class TransactionConflictRetries {
    private final SystemNanoClock clock;

    public TransactionConflictRetries(SystemNanoClock clock) {
        this.clock = requireNonNull(clock);
    }

    public <T> T retry(int maxRetries, Duration timeout, Operation<T> operation) {
        long timeoutNanos = timeout.toNanos();
        long deadline = timeoutNanos <= 0 ? 0 : clock.nanos() + timeoutNanos;
        long timeoutMillis = timeout.toMillis();
        int retriesLeft = maxRetries;
        while (true) {
            try {
                return operation.execute(timeoutMillis);
            } catch (TransactionConflictException e) {
                if (retriesLeft == 0) {
                    throw e;
                }
                if (deadline != 0) {
                    timeoutMillis = NANOSECONDS.toMillis(deadline - clock.nanos());
                    if (timeoutMillis <= 0) {
                        throw e;
                    }
                }
                retriesLeft--;
            }
        }
    }

    @FunctionalInterface
    public interface Operation<T> {
        T execute(long timeoutMillis);
    }
}
