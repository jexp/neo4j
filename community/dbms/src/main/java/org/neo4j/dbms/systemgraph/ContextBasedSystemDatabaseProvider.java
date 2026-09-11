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

import java.util.Optional;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseContextProvider;
import org.neo4j.function.Suppliers;
import org.neo4j.graphdb.event.DatabaseEventContext;
import org.neo4j.graphdb.event.DatabaseEventListenerAdapter;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.monitoring.DatabaseEventListeners;

public class ContextBasedSystemDatabaseProvider extends DatabaseEventListenerAdapter implements SystemDatabaseProvider {
    private final DatabaseContextProvider<? extends DatabaseContext> databaseContextProvider;

    private volatile Suppliers.Lazy<Optional<Cache>> cache;

    public ContextBasedSystemDatabaseProvider(
            DatabaseContextProvider<? extends DatabaseContext> databaseContextProvider,
            DatabaseEventListeners databaseEventListeners) {
        this.databaseContextProvider = databaseContextProvider;
        resetCache();
        databaseEventListeners.registerDatabaseEventListener(this);
    }

    @Override
    public Optional<SystemDatabaseContext> optionalDatabaseContext() {
        return cache.get().map(Cache::systemDatabaseContext);
    }

    @Override
    public <T> Optional<T> dependency(Class<T> type) throws SystemDatabaseUnavailableException {
        return cache.get()
                .map(Cache::context)
                .map(DatabaseContext::dependencies)
                .flatMap(dep -> dep.resolveOptionalDependency(type));
    }

    @Override
    public void databaseCreate(DatabaseEventContext eventContext) {
        if (eventContext.getDatabaseName().equals(NamedDatabaseId.SYSTEM_DATABASE_NAME)) {
            resetCache();
        }
    }

    private void resetCache() {
        cache = Suppliers.lazySingleton(this::fetch);
    }

    private Optional<Cache> fetch() {
        return databaseContextProvider
                .getDatabaseContext(NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID)
                .map(ctx -> new Cache(
                        ctx,
                        new SystemDatabaseContext(
                                ctx.databaseFacade(),
                                ctx.database().getConfig(),
                                ctx.database().getClock())));
    }

    private record Cache(DatabaseContext context, SystemDatabaseContext systemDatabaseContext) {}
}
