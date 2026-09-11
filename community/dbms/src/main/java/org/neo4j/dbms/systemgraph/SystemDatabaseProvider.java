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
package org.neo4j.dbms.systemgraph;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.system_snapshot_query_retries;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_timeout;
import static org.neo4j.kernel.impl.transaction.TransactionConflictRetries.retry;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.time.SystemNanoClock;

@FunctionalInterface
public interface SystemDatabaseProvider {
    class SystemDatabaseUnavailableException extends RuntimeException {}

    class SystemDatabasePanickedException extends SystemDatabaseUnavailableException {}

    default GraphDatabaseAPI database() throws SystemDatabaseUnavailableException {
        return optionalDatabaseContext()
                .orElseThrow(SystemDatabaseUnavailableException::new)
                .databaseAPI();
    }

    Optional<SystemDatabaseContext> optionalDatabaseContext();

    default void execute(Consumer<Transaction> consumer) throws SystemDatabaseUnavailableException {
        query(tx -> {
            consumer.accept(tx);
            return this; // cannot return null
        });
    }

    default <T> T query(Function<Transaction, T> function) throws SystemDatabaseUnavailableException {
        return query(optionalDatabaseContext(), function, true).orElseThrow();
    }

    default <T> Optional<T> queryIfAvailable(Function<Transaction, T> function) {
        return query(optionalDatabaseContext(), function, false);
    }

    default <T> Optional<T> dependency(Class<T> type) {
        return optionalDatabaseContext()
                .flatMap(systemDb ->
                        systemDb.databaseAPI().getDependencyResolver().resolveOptionalDependency(type));
    }

    private static <T> Optional<T> query(
            @SuppressWarnings("OptionalUsedAsFieldOrParameterType") Optional<SystemDatabaseContext> databaseContext,
            Function<Transaction, T> function,
            boolean failOnUnavailable)
            throws SystemDatabaseUnavailableException {
        if (databaseContext.isEmpty()) {
            if (failOnUnavailable) {
                throw new SystemDatabaseUnavailableException();
            }
            return Optional.empty();
        }
        var systemDbContext = databaseContext.get();
        var systemDatabaseApi = systemDbContext.databaseAPI();
        if (failOnUnavailable) {
            if (!systemDatabaseApi.isAvailable(1000)) {
                if (!systemDatabaseApi
                        .getDependencyResolver()
                        .resolveOptionalDependency(DatabaseHealth.class)
                        .map(DatabaseHealth::hasNoPanic)
                        .orElse(true)) {
                    throw new SystemDatabasePanickedException();
                }
                throw new SystemDatabaseUnavailableException();
            }
        } else if (!systemDatabaseApi.isAvailable(0)) {
            return Optional.empty();
        }
        var config = systemDbContext.config();
        return Optional.of(retry(
                systemDbContext.clock(),
                config.get(system_snapshot_query_retries),
                config.get(transaction_timeout),
                timeoutMillis -> {
                    try (var tx = systemDatabaseApi.beginTx(timeoutMillis, MILLISECONDS)) {
                        var result = function.apply(tx);
                        tx.commit();
                        return result;
                    }
                }));
    }

    record SystemDatabaseContext(GraphDatabaseAPI databaseAPI, Config config, SystemNanoClock clock) {}
}
