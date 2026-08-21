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
package org.neo4j.kernel.impl.pagecache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_async_io;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.configuration.GraphDatabaseSettings.preallocate_store_files;
import static org.neo4j.io.pagecache.PageCache.PAGE_SIZE;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_WRITE_LOCK;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.impl.muninn.StoreFile;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogAssertions;
import org.neo4j.logging.NullLog;
import org.neo4j.memory.MemoryGroup;
import org.neo4j.memory.MemoryPools;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.time.Clocks;

@EphemeralTestDirectoryExtension
class ConfiguringPageCacheFactoryTest {
    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private TestDirectory testDirectory;

    private JobScheduler jobScheduler;

    @BeforeEach
    void setUp() {
        jobScheduler = new ThreadPoolJobScheduler();
    }

    @AfterEach
    void tearDown() {
        jobScheduler.close();
    }

    @Test
    void shouldFitAsManyPagesAsItCan() {
        // Given
        long pageCount = 60;
        long memory = MuninnPageCache.memoryRequiredForPages(pageCount);
        Config config = Config.defaults(pagecache_memory, memory);

        // When
        ConfiguringPageCacheFactory factory = new ConfiguringPageCacheFactory(
                fs,
                config,
                PageCacheTracer.NULL,
                NullLog.getInstance(),
                jobScheduler,
                Clocks.nanoClock(),
                new MemoryPools());

        // Then
        try (PageCache cache = factory.getOrCreatePageCache()) {
            assertThat(cache.pageSize()).isEqualTo(PAGE_SIZE);
            assertThat(cache.maxCachedPages()).isBetween(pageCount - 2, pageCount);
        }
    }

    @Test
    void createPageCacheWithoutPreallocationEnabled() throws IOException {
        Config config = Config.defaults(preallocate_store_files, false);

        ConfiguringPageCacheFactory factory = new ConfiguringPageCacheFactory(
                fs,
                config,
                PageCacheTracer.NULL,
                NullLog.getInstance(),
                jobScheduler,
                Clocks.nanoClock(),
                new MemoryPools());

        Path testFile = testDirectory.createFile("a");
        try (var cache = factory.getOrCreatePageCache();
                var file = cache.map(new StoreFile(testFile), PAGE_SIZE, "foo");
                var io = file.io(1024, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            int bigPageToExpand = 20021;
            assertDoesNotThrow(() -> io.next(bigPageToExpand));
            assertEquals(bigPageToExpand, file.getLastPageId());
        }
    }

    @Test
    void shouldDumpConfigurationWithUnspecifiedPageCacheMemorySetting() {
        Config config = Config.defaults();
        AssertableLogProvider logProvider = new AssertableLogProvider();
        ConfiguringPageCacheFactory factory = new ConfiguringPageCacheFactory(
                fs,
                config,
                PageCacheTracer.NULL,
                logProvider.getLog(ConfiguringPageCacheFactory.class),
                jobScheduler,
                Clocks.nanoClock(),
                new MemoryPools());

        factory.dumpConfiguration();

        LogAssertions.assertThat(logProvider).containsMessages("Page cache: <not specified>");
    }

    @Test
    void preTouchPageCacheMemory() {
        long memory = MuninnPageCache.memoryRequiredForPages(60);
        Config config = Config.newBuilder()
                .set(pagecache_memory, memory)
                .set(GraphDatabaseInternalSettings.page_cache_allocator_pre_touch, true)
                .build();
        AssertableLogProvider logProvider = new AssertableLogProvider();
        MemoryPools memoryPools = new MemoryPools();
        ConfiguringPageCacheFactory factory = new ConfiguringPageCacheFactory(
                fs,
                config,
                PageCacheTracer.NULL,
                logProvider.getLog(ConfiguringPageCacheFactory.class),
                jobScheduler,
                Clocks.nanoClock(),
                memoryPools);

        try (PageCache cache = factory.getOrCreatePageCache()) {
            assertThat(cache.maxCachedPages()).isBetween(58L, 60L);
            var pool = memoryPools.getPools().stream()
                    .filter(p -> p.group() == MemoryGroup.PAGE_CACHE)
                    .findFirst()
                    .orElseThrow();
            assertThat(pool.usedNative()).isLessThanOrEqualTo(memory + PAGE_SIZE); // extra page for victim
            assertThat(pool.usedNative()).isGreaterThan(memory - 4L * PAGE_SIZE);
            LogAssertions.assertThat(logProvider).containsMessages("Page cache memory pre-touch completed");
        }
    }

    @Test
    void preTouchedPageCacheMemoryMustStayWithinConfiguredSize() throws IOException {
        long memory = 6 * MuninnPageCache.memoryRequiredForPages(60) + 12345;
        Config config = Config.newBuilder()
                .set(pagecache_memory, memory)
                .set(GraphDatabaseInternalSettings.page_cache_allocator_pre_touch, true)
                .build();
        MemoryPools memoryPools = new MemoryPools();
        ConfiguringPageCacheFactory factory = new ConfiguringPageCacheFactory(
                fs, config, PageCacheTracer.NULL, NullLog.getInstance(), jobScheduler, Clocks.nanoClock(), memoryPools);

        Path testFile = testDirectory.createFile("a");
        try (PageCache cache = factory.getOrCreatePageCache()) {
            var pool = memoryPools.getPools().stream()
                    .filter(p -> p.group() == MemoryGroup.PAGE_CACHE)
                    .findFirst()
                    .orElseThrow();
            try (var file = cache.map(new StoreFile(testFile), PAGE_SIZE, "foo");
                    var io = file.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                for (int i = 0; i < cache.maxCachedPages(); i++) {
                    assertThat(io.next()).isTrue();
                }
            }
            assertThat(pool.usedNative()).isLessThanOrEqualTo(memory + PAGE_SIZE); // extra page for victim
            assertThat(pool.usedNative()).isGreaterThan(memory - 4L * PAGE_SIZE);
        }
    }

    @Test
    void logConfiguredAsyncMode() {
        Config config = Config.defaults(pagecache_async_io, true);

        AssertableLogProvider logProvider = new AssertableLogProvider();
        ConfiguringPageCacheFactory factory = new ConfiguringPageCacheFactory(
                fs,
                config,
                PageCacheTracer.NULL,
                logProvider.getLog(ConfiguringPageCacheFactory.class),
                jobScheduler,
                Clocks.nanoClock(),
                new MemoryPools());

        try (var cache = factory.getOrCreatePageCache()) {
            LogAssertions.assertThat(logProvider)
                    .containsMessages("Page cache is configured to use async IO provider, if available.");
        }
    }
}
