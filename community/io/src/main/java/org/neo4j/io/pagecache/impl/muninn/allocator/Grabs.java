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
package org.neo4j.io.pagecache.impl.muninn.allocator;

import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.neo4j.internal.helpers.NamedThreadFactory.daemon;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.neo4j.internal.nativeimpl.NativeCallResult;
import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.io.ByteUnit;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.time.Stopwatch;
import org.neo4j.util.concurrent.Futures;

final class Grabs implements AutoCloseable {
    private static final long MAX_TOUCH_RANGE = ByteUnit.gibiBytes(1);

    private final int pageSize;
    private final int maxPages;
    private final MemoryTracker memoryTracker;
    private final Grab.MetadataGrab metadata;
    private final Grab.PageGrab pages;
    private final long metadataAddress;
    private final long pagesBase;
    private int allocatedPages;

    Grabs(
            int maxPages,
            int pageSize,
            long metaDataBytesPerPage,
            int pageAlignment,
            boolean preTouch,
            MemoryTracker memoryTracker,
            Consumer<String> log) {
        this.pageSize = pageSize;
        this.maxPages = maxPages;
        this.memoryTracker = memoryTracker;
        try {
            this.metadata = new Grab.MetadataGrab((long) maxPages * metaDataBytesPerPage, memoryTracker);
            this.metadataAddress = metadata.base();
            this.pages = new Grab.PageGrab((long) maxPages * pageSize, pageAlignment, memoryTracker);
            this.pagesBase = pages.base();
            if (preTouch) {
                touchPages(pages, log);
            }
        } catch (Throwable t) {
            close();
            throw t;
        }
    }

    /**
     * @return the pointer to the single 8-byte-aligned metadata region, allocated at construction.
     */
    long metadataAddress() {
        return metadataAddress;
    }

    long allocatePage() {
        if (allocatedPages == maxPages) {
            throw new IllegalStateException("All " + maxPages + " pages are already allocated.");
        }
        long page = pagesBase + (long) allocatedPages++ * pageSize;
        UnsafeUtil.dirtyMemory(page, pageSize);
        return page;
    }

    private static void touchPages(Grab.PageGrab pages, Consumer<String> log) {
        var stopWatch = Stopwatch.start();
        touchInRanges(pages, log);
        log.accept("Page cache memory pre-touch completed. " + ByteUnit.bytesToString(pages.size())
                + " touched. Duration: " + stopWatch.elapsed(TimeUnit.MILLISECONDS) + " ms.");
    }

    private static void touchInRanges(Grab.PageGrab grab, Consumer<String> log) {
        int workers = Runtime.getRuntime().availableProcessors();
        long size = grab.size();
        long range = rangeSize(size, workers, UnsafeUtil.pageSize());
        var cursor = new AtomicLong();
        Set<String> errors = ConcurrentHashMap.newKeySet();
        try (var executor = newFixedThreadPool(workers, daemon("PageCachePreTouch"))) {
            var futures = new ArrayList<Future<?>>(workers);
            for (int i = 0; i < workers; i++) {
                futures.add(executor.submit(() -> claimAndTouch(grab, cursor, size, range, log, errors)));
            }
            awaitAll(futures);
        }
    }

    private static long rangeSize(long size, int workers, long osPageSize) {
        long roughSize = Math.min(MAX_TOUCH_RANGE, Math.ceilDiv(size, workers));
        return Math.ceilDiv(roughSize, osPageSize) * osPageSize;
    }

    private static void claimAndTouch(
            Grab.PageGrab grab, AtomicLong cursor, long size, long range, Consumer<String> log, Set<String> errors) {
        long offset;
        while ((offset = cursor.getAndAdd(range)) < size) {
            long length = Math.min(range, size - offset);
            grab.touch(offset, length, result -> logError(result, log, errors));
        }
    }

    private static void logError(NativeCallResult result, Consumer<String> log, Set<String> errors) {
        if (errors.add(result.getErrorCode() + ": " + result.getErrorMessage())) {
            log.accept(
                    "Failed to pre-populate page cache memory, falling back to touching pages one by one: " + result);
        }
    }

    private static void awaitAll(List<Future<?>> futures) {
        try {
            Futures.getAll(futures);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new IllegalStateException("Failed to pre-touch page cache memory.", e.getCause());
        }
    }

    @Override
    public void close() {
        if (pages != null) {
            pages.free(memoryTracker);
        }
        if (metadata != null) {
            metadata.free(memoryTracker);
        }
    }
}
