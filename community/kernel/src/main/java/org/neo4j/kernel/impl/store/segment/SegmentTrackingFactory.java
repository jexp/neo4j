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
package org.neo4j.kernel.impl.store.segment;

import static org.neo4j.configuration.GraphDatabaseInternalSettings.store_segment_size;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.store_segment_tracking;
import static org.neo4j.io.pagecache.segment.DatabaseSegmentTracker.EMPTY_DATABASE_SEGMENT_TRACKER;
import static org.neo4j.kernel.impl.store.segment.SegmentMetadataService.EMPTY_METADATA_SERVICE;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCacheOpenOptions;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.segment.DatabaseSegmentTracker;
import org.neo4j.io.pagecache.segment.PageCacheSegmentTracker;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.InternalLog;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreId;

public class SegmentTrackingFactory extends LifecycleAdapter {

    private final FileSystemAbstraction fs;
    private final DatabaseLayout databaseLayout;
    private final DatabaseHealth databaseHealth;
    private final InternalLog log;
    private final boolean trackSegments;
    private final boolean segmentedStore;
    private final DatabaseSegmentTracker segmentTracker;

    public SegmentTrackingFactory(
            StorageEngineFactory storageEngineFactory,
            FileSystemAbstraction fs,
            PageCache pageCache,
            DatabaseLayout databaseLayout,
            CursorContextFactory contextFactory,
            Config config,
            DatabaseReadOnlyChecker readOnlyChecker,
            DatabaseHealth databaseHealth,
            InternalLog log) {
        this.fs = fs;
        this.databaseLayout = databaseLayout;
        this.databaseHealth = databaseHealth;
        this.log = log;
        this.segmentedStore = !readOnlyChecker.isReadOnly()
                && isSegmentedStore(storageEngineFactory, fs, pageCache, databaseLayout, contextFactory, config, log);
        this.trackSegments = segmentedStore && config.get(store_segment_tracking);
        this.segmentTracker = trackSegments
                ? new PageCacheSegmentTracker(databaseLayout.databaseDirectory())
                : EMPTY_DATABASE_SEGMENT_TRACKER;
    }

    @Override
    public void init() {
        if (segmentedStore && !trackSegments) {
            deleteIfExists(fs, databaseLayout.segmentsMetadata(), log);
        }
    }

    public DatabaseSegmentTracker segmentTracker() {
        return segmentTracker;
    }

    public SegmentMetadataService createSegmentMetadataService(StoreId storeId, MemoryTracker memoryTracker) {
        return trackSegments
                ? new SegmentedStoreMetadataService(
                        fs, databaseLayout, storeId, segmentTracker, memoryTracker, databaseHealth, log)
                : EMPTY_METADATA_SERVICE;
    }

    private void deleteIfExists(FileSystemAbstraction fs, Path path, InternalLog log) {
        Path fileName = databaseLayout.segmentsMetadata().getFileName();
        try {
            Path[] segmentFiles = fs.listFiles(
                    databaseLayout.databaseDirectory(),
                    entry -> entry.toFile().isFile() && entry.getFileName().startsWith(fileName));
            for (Path segmentFile : segmentFiles) {
                fs.deleteFile(segmentFile);
            }
        } catch (IOException e) {
            log.warn("Error on deletion of segments metadata file " + path + ".", e);
        }
    }

    private static boolean isSegmentedStore(
            StorageEngineFactory storageEngineFactory,
            FileSystemAbstraction fs,
            PageCache pageCache,
            DatabaseLayout databaseLayout,
            CursorContextFactory contextFactory,
            Config config,
            InternalLog log) {
        if (!storageEngineFactory.storageExists(fs, databaseLayout)) {
            return storageEngineFactory.name().contains("block") && config.get(store_segment_size) != 0;
        }
        try {
            var storeOpenOptions =
                    storageEngineFactory.getStoreOpenOptions(fs, pageCache, databaseLayout, contextFactory);
            for (OpenOption openOption : storeOpenOptions) {
                if (openOption instanceof PageCacheOpenOptions.SegmentedOpenOption) {
                    return true;
                }
            }
        } catch (Exception e) {
            log.warn("Fail to detect if store is segmented. Fallback to false.", e);
        }
        return false;
    }
}
