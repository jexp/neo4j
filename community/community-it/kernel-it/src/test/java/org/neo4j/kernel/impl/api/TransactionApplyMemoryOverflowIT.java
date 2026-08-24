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
package org.neo4j.kernel.impl.api;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.graphdb.factory.module.edition.CommunityEditionModule;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.factory.DefaultTransactionalProcessFactory;
import org.neo4j.kernel.impl.transaction.tracing.TransactionWriteEvent;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.LogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageEngineTransaction;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.EphemeralNeo4jLayoutExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.wal.TransactionAppender;

@EphemeralNeo4jLayoutExtension
class TransactionApplyMemoryOverflowIT {
    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private DatabaseLayout databaseLayout;

    @Test
    void memoryOverflowDuringStoreApplyDoesNotPanicDatabase() {
        AtomicBoolean injectOverflowOnApply = new AtomicBoolean();
        try (DatabaseManagementService managementService = new FaultInjectingBuilder(
                        databaseLayout.databaseDirectory(), fs, injectOverflowOnApply)
                .setConfig(GraphDatabaseSettings.memory_transaction_max_size, ByteUnit.mebiBytes(8))
                .build()) {
            GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
            DatabaseHealth databaseHealth = db.getDependencyResolver().resolveDependency(DatabaseHealth.class);

            try (Transaction tx = db.beginTx()) {
                tx.createNode();
                tx.commit();
            }
            assertTrue(databaseHealth.hasNoPanic(), "sanity commit under the configured limit must not panic");

            injectOverflowOnApply.set(true);
            assertDoesNotThrow(() -> {
                try (Transaction tx = db.beginTx()) {
                    tx.createNode();
                    tx.commit();
                }
            });

            assertTrue(databaseHealth.hasNoPanic(), "a memory overflow during store apply must not panic the database");
        }
    }

    private static class FaultInjectingBuilder extends TestDatabaseManagementServiceBuilder {
        private final AtomicBoolean injectOverflowOnApply;

        FaultInjectingBuilder(Path homeDirectory, FileSystemAbstraction fs, AtomicBoolean injectOverflowOnApply) {
            super(homeDirectory);
            setFileSystem(fs);
            this.injectOverflowOnApply = injectOverflowOnApply;
        }

        @Override
        protected Function<GlobalModule, AbstractEditionModule> getEditionFactory(Config config) {
            return globalModule -> new CommunityEditionModule(globalModule) {
                @Override
                protected TransactionalProcessFactory createCommitProcessFactory() {
                    return new FaultInjectingTransactionalProcessFactory(injectOverflowOnApply);
                }
            };
        }
    }

    private static class FaultInjectingTransactionalProcessFactory extends DefaultTransactionalProcessFactory {
        private final AtomicBoolean injectOverflowOnApply;

        FaultInjectingTransactionalProcessFactory(AtomicBoolean injectOverflowOnApply) {
            this.injectOverflowOnApply = injectOverflowOnApply;
        }

        @Override
        public TransactionCommitProcess create(
                TransactionAppender appender,
                StorageEngine storageEngine,
                DatabaseReadOnlyChecker readOnlyChecker,
                boolean preAllocateSpaceInStoreFiles,
                CommandCommitListeners commandCommitListeners,
                boolean prefetchPages,
                LogProvider logProvider) {
            return new DatabaseTransactionCommitProcess(
                    new InternalTransactionCommitProcess(
                            appender,
                            storageEngine,
                            preAllocateSpaceInStoreFiles,
                            commandCommitListeners,
                            () -> prefetchPages,
                            logProvider) {
                        @Override
                        protected void applyToStore(
                                StorageEngineTransaction batch,
                                TransactionWriteEvent transactionWriteEvent,
                                TransactionApplicationMode mode,
                                MemoryTracker memoryTracker)
                                throws TransactionFailureException {
                            if (injectOverflowOnApply.get()) {
                                memoryTracker.allocateHeap(ByteUnit.mebiBytes(64));
                            }
                            super.applyToStore(batch, transactionWriteEvent, mode, memoryTracker);
                        }
                    },
                    readOnlyChecker);
        }
    }
}
