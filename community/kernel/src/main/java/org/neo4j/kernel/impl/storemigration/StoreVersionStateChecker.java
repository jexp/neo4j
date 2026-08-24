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
package org.neo4j.kernel.impl.storemigration;

import static org.neo4j.kernel.impl.storemigration.StoreMigrator.checkNoBlockingMigrationAndCleanupIfNonBlocking;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_TX_ID;

import java.io.IOException;
import java.util.function.Supplier;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.context.FixedVersionContextSupplier;
import org.neo4j.io.pagecache.context.VersionContext;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreVersionCheck;
import org.neo4j.wal.LogTailMetadata;

public class StoreVersionStateChecker {
    private StoreVersionStateChecker() {}

    public static void checkVersionSupportedAndNoBlockingInterruptedMigration(
            FileSystemAbstraction fs,
            Config config,
            LogService logService,
            PageCache pageCache,
            DatabaseTracers databaseTracers,
            DatabaseLayout databaseLayout,
            StorageEngineFactory storageEngineFactory,
            MemoryTracker memoryTracker,
            Supplier<LogTailMetadata> logTailSupplier)
            throws IOException {

        if (!storageEngineFactory.storageExists(fs, databaseLayout)) {
            // upgrade check is invoked on database start up and before new databases are initialised,
            // so the database store not existing is a perfectly valid scenario.
            return;
        }

        checkNoBlockingMigrationAndCleanupIfNonBlocking(databaseLayout, fs, memoryTracker);

        CursorContextFactory contextFactory = new CursorContextFactory(
                databaseTracers.getPageCacheTracer(), new LazyVersionContextSupplier(logTailSupplier));
        try (var cursorContext = contextFactory.create("storeStateCheck")) {
            StoreVersionCheck storeVersionCheck = storageEngineFactory.versionCheck(
                    fs, databaseLayout, config, pageCache, logService, contextFactory);
            var checkResult = storeVersionCheck.getAndCheckUpgradeTargetVersion(cursorContext);
            switch (checkResult.outcome()) {
                case UPGRADE_POSSIBLE ->
                    // Upgrade on start up is not supported anymore - this should never happen unless we accidentally
                    // introduce a new minor version, but let's throw something to discover if it does.
                    throw new IllegalStateException(
                            "Current store version has a descendant which is not supported. Seen versions %s -> %s."
                                    .formatted(
                                            checkResult.versionToUpgradeFrom().getStoreVersionUserString(),
                                            checkResult.versionToUpgradeTo().getStoreVersionUserString()));
                case NO_OP -> {}
                case STORE_VERSION_RETRIEVAL_FAILURE ->
                    throw new IllegalStateException("Failed to read current store version.", checkResult.cause());
                case UNSUPPORTED_TARGET_VERSION ->
                    throw new UnableToMigrateException(String.format(
                            "The selected target store format '%s' (introduced in %s) is no longer supported",
                            checkResult.versionToUpgradeTo().getStoreVersionUserString(),
                            storeVersionCheck.getIntroductionVersionFromVersion(checkResult.versionToUpgradeTo())));
            }
        }
    }

    private static class LazyVersionContextSupplier extends FixedVersionContextSupplier {

        private final Supplier<LogTailMetadata> logTailSupplier;

        public LazyVersionContextSupplier(Supplier<LogTailMetadata> logTailSupplier) {
            super(UNKNOWN_TX_ID);
            this.logTailSupplier = logTailSupplier;
        }

        @Override
        public VersionContext createVersionContext() {
            return new StoreMigrator.LazyTailVersionContext(logTailSupplier);
        }
    }
}
