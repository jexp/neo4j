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
package org.neo4j.kernel.impl.factory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.snapshot_query_retries;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.system_snapshot_query_retries;
import static org.neo4j.kernel.database.NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.HostedOnMode;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.ResultTransformer;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.connectioninfo.RoutingInfo;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.TransactionExceptionMapper;
import org.neo4j.storageengine.api.txstate.validation.TransactionConflictException;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

class GraphDatabaseTransactionsTest {
    private static final NamedDatabaseId USER_DATABASE_ID = DatabaseIdFactory.from("neo4j", UUID.randomUUID());
    private static final BooleanSupplier MULTI_VERSIONED = () -> true;
    private static final BooleanSupplier SINGLE_VERSION = () -> false;

    private final FakeClock clock = Clocks.fakeClock();

    @Test
    void retryUntilSuccessOnMultiVersionedStore() {
        var database =
                new TestDatabase(Config.defaults(), MULTI_VERSIONED, USER_DATABASE_ID, transactionFailingTimes(3));

        database.executeTransactionally("RETURN 1");

        assertThat(database.transactionsStarted).isEqualTo(4);
    }

    @Test
    void giveUpAfterConfiguredRetriesOnMultiVersionedStore() {
        var config = Config.defaults(snapshot_query_retries, 3);
        var database = new TestDatabase(config, MULTI_VERSIONED, USER_DATABASE_ID, alwaysFailingTransaction());

        assertThatThrownBy(() -> database.executeTransactionally("RETURN 1"))
                .isInstanceOf(TransactionConflictException.class);
        assertThat(database.transactionsStarted).isEqualTo(4);
    }

    @Test
    void systemDatabaseUsesSystemRetrySetting() {
        var config = Config.newBuilder()
                .set(snapshot_query_retries, 3)
                .set(system_snapshot_query_retries, 7)
                .build();
        var database = new TestDatabase(config, MULTI_VERSIONED, NAMED_SYSTEM_DATABASE_ID, alwaysFailingTransaction());

        assertThatThrownBy(() -> database.executeTransactionally("RETURN 1"))
                .isInstanceOf(TransactionConflictException.class);
        assertThat(database.transactionsStarted).isEqualTo(8);
    }

    @Test
    void noRetryOnSingleVersionStore() {
        var database =
                new TestDatabase(Config.defaults(), SINGLE_VERSION, USER_DATABASE_ID, alwaysFailingTransaction());

        assertThatThrownBy(() -> database.executeTransactionally("RETURN 1"))
                .isInstanceOf(TransactionConflictException.class);
        assertThat(database.transactionsStarted).isEqualTo(1);
    }

    @Test
    void retriesRespectTransactionTimeout() {
        var database =
                new TestDatabase(Config.defaults(), MULTI_VERSIONED, USER_DATABASE_ID, alwaysFailingTransaction());
        var attemptDuration = Duration.ofSeconds(4);
        doAnswer(invocation -> {
                    clock.forward(attemptDuration);
                    throw TransactionConflictException.transactionConflict(new Exception("conflict"));
                })
                .when(database.transaction)
                .commit();

        assertThatThrownBy(() -> database.executeTransactionally(
                        "RETURN 1", Map.of(), ResultTransformer.EMPTY_TRANSFORMER, Duration.ofSeconds(10)))
                .isInstanceOf(TransactionConflictException.class);

        assertThat(database.transactionTimeouts).containsExactly(10_000L, 6_000L, 2_000L);
    }

    @Test
    void zeroTransactionTimeoutDoesNotLimitRetries() {
        var database =
                new TestDatabase(Config.defaults(), MULTI_VERSIONED, USER_DATABASE_ID, alwaysFailingTransaction());
        var remaining = new AtomicInteger(3);
        doAnswer(invocation -> {
                    clock.forward(Duration.ofDays(1));
                    if (remaining.getAndDecrement() > 0) {
                        throw TransactionConflictException.transactionConflict(new Exception("conflict"));
                    }
                    return null;
                })
                .when(database.transaction)
                .commit();

        database.executeTransactionally("RETURN 1", Map.of(), ResultTransformer.EMPTY_TRANSFORMER, Duration.ZERO);

        assertThat(database.transactionsStarted).isEqualTo(4);
        assertThat(database.transactionTimeouts).containsExactly(0L, 0L, 0L, 0L);
    }

    private static InternalTransaction alwaysFailingTransaction() {
        return transactionFailingTimes(Integer.MAX_VALUE);
    }

    private static InternalTransaction transactionFailingTimes(int failures) {
        var transaction = mock(InternalTransaction.class);
        when(transaction.execute(anyString(), anyMap())).thenReturn(mock(Result.class));
        var remaining = new AtomicInteger(failures);
        doAnswer(invocation -> {
                    if (remaining.getAndDecrement() > 0) {
                        throw TransactionConflictException.transactionConflict(new Exception("conflict"));
                    }
                    return null;
                })
                .when(transaction)
                .commit();
        return transaction;
    }

    private class TestDatabase extends GraphDatabaseTransactions {
        private final NamedDatabaseId databaseId;
        private final InternalTransaction transaction;
        private final List<Long> transactionTimeouts = new ArrayList<>();
        private int transactionsStarted;

        TestDatabase(
                Config config,
                BooleanSupplier multiVersioned,
                NamedDatabaseId databaseId,
                InternalTransaction transaction) {
            super(config, clock, databaseId.databaseId(), multiVersioned);
            this.databaseId = databaseId;
            this.transaction = transaction;
        }

        @Override
        public InternalTransaction beginTransaction(
                KernelTransaction.Type type,
                LoginContext loginContext,
                ClientConnectionInfo clientInfo,
                long timeout,
                TimeUnit unit) {
            transactionsStarted++;
            transactionTimeouts.add(unit.toMillis(timeout));
            return transaction;
        }

        @Override
        public InternalTransaction beginTransaction(
                KernelTransaction.Type type,
                LoginContext loginContext,
                ClientConnectionInfo clientInfo,
                RoutingInfo routingInfo,
                List<String> bookmarks,
                long timeout,
                TimeUnit unit,
                Consumer<Status> terminationCallback,
                TransactionExceptionMapper transactionExceptionMapper) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DependencyResolver getDependencyResolver() {
            throw new UnsupportedOperationException();
        }

        @Override
        public NamedDatabaseId databaseId() {
            return databaseId;
        }

        @Override
        public String databaseName() {
            return databaseId.name();
        }

        @Override
        public boolean isAvailable() {
            return true;
        }

        @Override
        public boolean isAvailable(long timeoutMillis) {
            return true;
        }

        @Override
        public DatabaseLayout databaseLayout() {
            throw new UnsupportedOperationException();
        }

        @Override
        public DbmsInfo dbmsInfo() {
            throw new UnsupportedOperationException();
        }

        @Override
        public HostedOnMode mode() {
            throw new UnsupportedOperationException();
        }
    }
}
