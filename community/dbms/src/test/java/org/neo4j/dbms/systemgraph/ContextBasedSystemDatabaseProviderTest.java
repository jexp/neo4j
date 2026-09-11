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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.database.NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID;

import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseContextProvider;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.monitoring.DatabaseEventListeners;
import org.neo4j.logging.NullLog;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.time.Clocks;

class ContextBasedSystemDatabaseProviderTest {
    private DatabaseContext context;
    private GraphDatabaseAPI database;
    private DatabaseContextProvider<DatabaseContext> contextProvider;
    private ContextBasedSystemDatabaseProvider provider;
    private DatabaseEventListeners listeners;

    @BeforeEach
    void setup() {
        context = mock(DatabaseContext.class);
        database = mock(GraphDatabaseAPI.class);
        when(context.dependencies()).thenReturn(Dependencies.dependenciesOf(database));

        var kernelDatabase = mock(Database.class);
        when(kernelDatabase.getConfig()).thenReturn(Config.defaults());
        when(kernelDatabase.getClock()).thenReturn(Clocks.nanoClock());
        when(context.database()).thenReturn(kernelDatabase);
        when(context.databaseFacade()).thenReturn(database);
        contextProvider = mock(DatabaseContextProvider.class);
        when(contextProvider.getDatabaseContext(any(NamedDatabaseId.class))).thenReturn(Optional.of(context));
        listeners = new DatabaseEventListeners(NullLog.getInstance());
        provider = new ContextBasedSystemDatabaseProvider(contextProvider, listeners);
    }

    @Test
    void shouldUseContextDependencies() {
        // when
        assertThat(provider.dependency(GraphDatabaseAPI.class)).hasValue(database);

        // then
        verify(database, never()).getDependencyResolver();
        verify(context).dependencies();
        clearInvocations(context, database);

        // when
        assertThat(provider.dependency(TransactionIdStore.class)).isEmpty();

        // then
        verify(database, never()).getDependencyResolver();
        verify(context).dependencies();
    }

    @Test
    void shouldResetWhenDatabaseCreated() {
        // when
        provider.database();
        provider.database();
        provider.database();

        // then
        verify(contextProvider, times(1)).getDatabaseContext(NAMED_SYSTEM_DATABASE_ID);
        verify(context, times(1)).databaseFacade();
        verify(context, times(2)).database();
        verifyNoMoreInteractions(context, contextProvider);
        clearInvocations(context, contextProvider);

        // when
        provider.database();
        provider.database();
        provider.database();

        // then
        verifyNoInteractions(context, contextProvider);

        // when
        listeners.databaseCreate(NAMED_SYSTEM_DATABASE_ID);
        provider.database();
        provider.database();
        provider.database();

        // then
        verify(contextProvider, times(1)).getDatabaseContext(NAMED_SYSTEM_DATABASE_ID);
        verify(context, times(1)).databaseFacade();
        verify(context, times(2)).database();
        verifyNoMoreInteractions(context, contextProvider);
    }
}
