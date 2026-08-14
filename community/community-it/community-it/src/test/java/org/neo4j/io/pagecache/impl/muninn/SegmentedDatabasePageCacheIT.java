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
package org.neo4j.io.pagecache.impl.muninn;

import static java.nio.file.StandardOpenOption.CREATE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.collections.api.factory.Sets.immutable;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

import java.io.IOException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DatabasePageCache;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOController;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.segment.PageCacheSegmentTracker;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
public class SegmentedDatabasePageCacheIT {
    @Inject
    private TestDirectory directory;

    @Inject
    private FileSystemAbstraction fs;

    private JobScheduler jobScheduler;
    private final LifeSupport life = new LifeSupport();
    private PageCacheSegmentTracker databaseSegmentTracker;

    @BeforeEach
    void start() {
        databaseSegmentTracker = new PageCacheSegmentTracker(directory.homePath());
        jobScheduler = JobSchedulerFactory.createScheduler();
        life.add(jobScheduler);
        life.start();
    }

    @AfterEach
    void stop() {
        life.shutdown();
    }

    @Test
    void registerSegmentedProfilesOnFileMap() throws IOException {
        try (DatabasePageCache pageCache = createPageCache()) {
            assertThat(databaseSegmentTracker.segmentMetadata()).isEmpty();

            try (PagedFile pagedFileA =
                    pageCache.map(new StoreFile(directory.file("a")), DEFAULT_DATABASE_NAME, immutable.of(CREATE))) {
                assertThat(databaseSegmentTracker.segmentMetadata()).hasSize(1);
                try (PagedFile pagedFileB = pageCache.map(
                        new StoreFile(directory.file("b")), DEFAULT_DATABASE_NAME, immutable.of(CREATE))) {
                    assertThat(databaseSegmentTracker.segmentMetadata()).hasSize(2);
                }
            }
        }
    }

    @Test
    void removeSegmentedProfilesOnFileUnmap() throws IOException {
        try (DatabasePageCache pageCache = createPageCache()) {
            try (PagedFile pagedFile =
                    pageCache.map(new StoreFile(directory.file("a")), DEFAULT_DATABASE_NAME, immutable.of(CREATE))) {
                assertThat(databaseSegmentTracker.segmentMetadata()).hasSize(1);
            }

            assertThat(databaseSegmentTracker.segmentMetadata()).isEmpty();
        }
    }

    @Test
    void keepSegmentedProfilesUntilLastFileUnmap() throws IOException {
        try (DatabasePageCache pageCache = createPageCache()) {
            try (PagedFile pagedFile =
                    pageCache.map(new StoreFile(directory.file("a")), DEFAULT_DATABASE_NAME, immutable.of(CREATE))) {
                try (PagedFile remappedFile = pageCache.map(
                        new StoreFile(directory.file("a")), DEFAULT_DATABASE_NAME, immutable.of(CREATE))) {
                    assertThat(databaseSegmentTracker.segmentMetadata()).hasSize(1);
                }
                assertThat(databaseSegmentTracker.segmentMetadata()).hasSize(1);
            }
            assertThat(databaseSegmentTracker.segmentMetadata()).isEmpty();
        }
    }

    private DatabasePageCache createPageCache() {
        return new DatabasePageCache(
                new MuninnPageCache(fs, jobScheduler, MuninnPageCache.config(1_000)),
                IOController.DISABLED,
                VersionStorage.EMPTY_STORAGE,
                databaseSegmentTracker,
                Config.defaults());
    }
}
