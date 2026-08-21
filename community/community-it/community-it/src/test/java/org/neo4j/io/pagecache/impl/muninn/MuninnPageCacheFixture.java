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

import java.util.concurrent.CountDownLatch;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.pagecache.ConfigurableIOBufferFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCacheTestSupport;
import org.neo4j.io.pagecache.buffer.IOBufferFactory;
import org.neo4j.io.pagecache.impl.muninn.swapper.PageSwapperFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.scheduler.JobScheduler;

public class MuninnPageCacheFixture extends PageCacheTestSupport.Fixture<MuninnPageCache> {
    CountDownLatch backgroundFlushLatch;

    @Override
    public MuninnPageCache createPageCache(
            FileSystemAbstraction fs,
            int maxPages,
            PageCacheTracer tracer,
            JobScheduler jobScheduler,
            IOBufferFactory bufferFactory,
            PageSwapperFactory swapperFactory) {
        int reservedBytes = getReservedBytes();
        var memoryTracker = new LocalMemoryTracker();
        var configuration = MuninnPageCache.forPages(maxPages)
                .memoryTracker(memoryTracker)
                .pageCacheTracer(tracer)
                .bufferFactory(selectBufferFactory(bufferFactory, memoryTracker))
                .swapperFactory(swapperFactory)
                .reservedPageBytes(reservedBytes)
                .withAsyncIO(asyncIO())
                .closeAllocatorOnShutdown(closeAllocatorOnShutdown());
        if (!backgroundEvictionEnabled()) {
            configuration.disableEvictionThread();
        }
        return new MuninnPageCache(fs, jobScheduler, configuration);
    }

    private static IOBufferFactory selectBufferFactory(
            IOBufferFactory bufferFactory, LocalMemoryTracker memoryTracker) {
        return bufferFactory != null
                ? bufferFactory
                : new ConfigurableIOBufferFactory(Config.defaults(), memoryTracker);
    }

    @Override
    public void tearDownPageCache(MuninnPageCache pageCache) {
        if (backgroundFlushLatch != null) {
            backgroundFlushLatch.countDown();
            backgroundFlushLatch = null;
        }
        pageCache.close();
    }
}
