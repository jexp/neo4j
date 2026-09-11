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

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.snapshot_query_retries;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.system_snapshot_query_retries;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_timeout;
import static org.neo4j.graphdb.ResultTransformer.EMPTY_TRANSFORMER;
import static org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo.EMBEDDED_CONNECTION;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.kernel.impl.transaction.TransactionConflictRetries.retry;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.ResultTransformer;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.time.SystemNanoClock;

/**
 * Implements all special versions of "begin" (beginTx, beginTransaction, executeTransactionally)
 * by delegating to a single abstract beginTransaction()
 */
public abstract class GraphDatabaseTransactions implements GraphDatabaseAPI {
    private final Config config;
    private final SystemNanoClock clock;
    private final BooleanSupplier multiVersioned;
    private final int multiVersionRetries;

    protected GraphDatabaseTransactions(
            Config config, SystemNanoClock clock, DatabaseId databaseId, BooleanSupplier multiVersioned) {
        this.config = requireNonNull(config);
        this.multiVersioned = requireNonNull(multiVersioned);
        this.clock = requireNonNull(clock);
        this.multiVersionRetries = databaseId.isSystemDatabase()
                ? config.get(system_snapshot_query_retries)
                : config.get(snapshot_query_retries);
    }

    @Override
    public void executeTransactionally(String query) throws QueryExecutionException {
        executeTransactionally(query, emptyMap(), EMPTY_TRANSFORMER);
    }

    @Override
    public void executeTransactionally(String query, Map<String, Object> parameters) throws QueryExecutionException {
        executeTransactionally(query, parameters, EMPTY_TRANSFORMER);
    }

    @Override
    public <T> T executeTransactionally(
            String query, Map<String, Object> parameters, ResultTransformer<T> resultTransformer)
            throws QueryExecutionException {
        return executeTransactionally(query, parameters, resultTransformer, defaultTransactionTimeout());
    }

    @Override
    public <T> T executeTransactionally(
            String query, Map<String, Object> parameters, ResultTransformer<T> resultTransformer, Duration timeout)
            throws QueryExecutionException {
        return retry(clock, maxRetries(), timeout, timeoutMillis -> {
            try (var internalTransaction = beginTransaction(
                    KernelTransaction.Type.IMPLICIT, AUTH_DISABLED, EMBEDDED_CONNECTION, timeoutMillis, MILLISECONDS)) {
                T transformedResult;
                try (var result = internalTransaction.execute(query, parameters)) {
                    transformedResult = resultTransformer.apply(result);
                }
                internalTransaction.commit();
                return transformedResult;
            }
        });
    }

    @Override
    public Transaction beginTx() {
        return beginTransaction();
    }

    @Override
    public Transaction beginTx(long timeout, TimeUnit unit) {
        return beginTransaction(KernelTransaction.Type.EXPLICIT, AUTH_DISABLED, EMBEDDED_CONNECTION, timeout, unit);
    }

    @Override
    public InternalTransaction beginTransaction(KernelTransaction.Type type, LoginContext loginContext) {
        return beginTransaction(type, loginContext, EMBEDDED_CONNECTION);
    }

    @Override
    public InternalTransaction beginTransaction(
            KernelTransaction.Type type, LoginContext loginContext, ClientConnectionInfo clientInfo) {
        return beginTransaction(
                type, loginContext, clientInfo, defaultTransactionTimeout().toMillis(), MILLISECONDS);
    }

    @Override
    public abstract InternalTransaction beginTransaction(
            KernelTransaction.Type type,
            LoginContext loginContext,
            ClientConnectionInfo clientInfo,
            long timeout,
            TimeUnit unit);

    protected Duration defaultTransactionTimeout() {
        return config.get(transaction_timeout);
    }

    private int maxRetries() {
        return multiVersioned.getAsBoolean() ? multiVersionRetries : 0;
    }
}
