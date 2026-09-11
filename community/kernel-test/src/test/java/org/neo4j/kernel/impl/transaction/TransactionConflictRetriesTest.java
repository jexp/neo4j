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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.kernel.impl.transaction.TransactionConflictRetries.retry;

import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.storageengine.api.txstate.validation.TransactionConflictException;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

class TransactionConflictRetriesTest {
    private final FakeClock clock = Clocks.fakeClock();

    @Test
    void returnResultWithoutRetryOnSuccess() {
        var attempts = new AtomicInteger();

        var result = retry(clock, 5, Duration.ZERO, timeoutMillis -> {
            attempts.incrementAndGet();
            return "done";
        });

        assertThat(result).isEqualTo("done");
        assertThat(attempts).hasValue(1);
    }

    @Test
    void retryUntilSuccess() {
        var attempts = new AtomicInteger();

        var result = retry(clock, 5, Duration.ZERO, timeoutMillis -> {
            if (attempts.incrementAndGet() < 4) {
                throw conflict();
            }
            return attempts.get();
        });

        assertThat(result).isEqualTo(4);
        assertThat(attempts).hasValue(4);
    }

    @Test
    void rethrowLastFailureWhenRetriesExhausted() {
        var attempts = new AtomicInteger();
        var lastFailure = new AtomicReference<TransactionConflictException>();

        assertThatThrownBy(() -> retry(clock, 3, Duration.ZERO, timeoutMillis -> {
                    attempts.incrementAndGet();
                    var failure = conflict();
                    lastFailure.set(failure);
                    throw failure;
                }))
                .isSameAs(lastFailure.get());

        assertThat(attempts).hasValue(4);
    }

    @Test
    void noRetryWithZeroMaxRetries() {
        var attempts = new AtomicInteger();

        assertThatThrownBy(() -> retry(clock, 0, Duration.ZERO, timeoutMillis -> {
                    attempts.incrementAndGet();
                    throw conflict();
                }))
                .isInstanceOf(TransactionConflictException.class);

        assertThat(attempts).hasValue(1);
    }

    @Test
    void failNonConflictTransientFailureImmediately() {
        var attempts = new AtomicInteger();

        assertThatThrownBy(() -> retry(clock, 5, Duration.ZERO, timeoutMillis -> {
                    attempts.incrementAndGet();
                    throw TransientTransactionFailureException.outdatedRead();
                }))
                .isInstanceOf(TransientTransactionFailureException.class);

        assertThat(attempts).hasValue(1);
    }

    @Test
    void failNonTransientFailureImmediately() {
        var attempts = new AtomicInteger();

        assertThatThrownBy(() -> retry(clock, 5, Duration.ZERO, timeoutMillis -> {
                    attempts.incrementAndGet();
                    throw new IllegalStateException("not transient");
                }))
                .isInstanceOf(IllegalStateException.class);

        assertThat(attempts).hasValue(1);
    }

    @Test
    void handRemainingTimeoutToRetriedAttempts() {
        var timeouts = new ArrayList<Long>();

        assertThatThrownBy(() -> retry(clock, 10, Duration.ofSeconds(10), timeoutMillis -> {
                    timeouts.add(timeoutMillis);
                    clock.forward(Duration.ofSeconds(4));
                    throw conflict();
                }))
                .isInstanceOf(TransactionConflictException.class);

        // the deadline runs out before the retry budget of 10 does
        assertThat(timeouts).containsExactly(10_000L, 6_000L, 2_000L);
    }

    @Test
    void zeroTimeoutDoesNotLimitRetries() {
        var attempts = new AtomicInteger();
        var timeouts = new ArrayList<Long>();

        var result = retry(clock, 2, Duration.ZERO, timeoutMillis -> {
            timeouts.add(timeoutMillis);
            clock.forward(Duration.ofDays(1));
            if (attempts.incrementAndGet() < 3) {
                throw conflict();
            }
            return attempts.get();
        });

        assertThat(result).isEqualTo(3);
        assertThat(timeouts).containsExactly(0L, 0L, 0L);
    }

    private static TransactionConflictException conflict() {
        return TransactionConflictException.transactionConflict(new Exception("conflict"));
    }
}
