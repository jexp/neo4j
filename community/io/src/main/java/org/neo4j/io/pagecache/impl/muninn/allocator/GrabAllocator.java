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

import static java.lang.String.format;
import static org.neo4j.util.Preconditions.requirePositive;

import java.lang.ref.Cleaner;
import java.util.function.Consumer;
import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.memory.MemoryTracker;

/**
 * Native memory allocator built for the {@link MuninnPageCache}.
 * All memory is allocated upfront on construction, as two grabs: a dedicated region for page metadata, and a region
 * hosting all page buffers.
 *
 * metadata grab                 pages grab
 * ┌─────────────────┐   ┌───┬─────┬─────┬───┬─────┐
 * │  page metadata, │   │pad│ page│ page│...│ page│
 * │  one entry/page │   │   │  0  │  1  │   │ n-1 │
 * └─────────────────┘   └───┴─────┴─────┴───┴─────┘
 *
 * malloc does not guarantee a page-aligned address, so pages grab is allocated with bufferAlignment - 1 extra bytes and an
 * aligned base address is chosen inside it. Page buffers are handed out sequentially from the base as the page cache
 * faults pages in.
 *
 * In pre-touch mode the pages grab is additionally touched on construction, to ensure that all requested memory is
 * faulted in upfront.
 *
 * Allocation is sized upfront based on the requested page count or memory budget.
 * If created based on a memory budget, the budget is never exceeded.
 */
public final class GrabAllocator implements AutoCloseable {

    private static final int MINIMUM_PAGE_COUNT = 2;
    private static final int MAX_PAGES = Integer.MAX_VALUE;

    private static final Cleaner GLOBAL_CLEANER = globalCleaner();

    private final Grabs grabs;
    private final Cleaner.Cleanable cleanable;
    private final int maxPages;

    /**
     * Create an allocator for a page cache, sized either by an explicit page count or by a memory budget. Exactly one
     * of {@code requestedMaxPages} and {@code requestedMaxMemory} must be given.
     *
     * @param requestedMaxPages    the exact number of pages to hold, or {@code null} to size by memory.
     * @param requestedMaxMemory   the memory budget in bytes, or {@code null} to size by page count.
     * @param metaDataBytesPerPage the size, in bytes, of the metadata kept for each page.
     * @param pageSize             the cache page size, in bytes.
     * @param memoryTracker        memory usage tracker.
     * @param preTouch             touch all the allocated memory upfront to fault it in.
     * @param log                  consumer of log messages.
     */
    public static GrabAllocator createAllocator(
            Integer requestedMaxPages,
            Long requestedMaxMemory,
            int metaDataBytesPerPage,
            int pageSize,
            MemoryTracker memoryTracker,
            boolean preTouch,
            Consumer<String> log) {
        requirePositive(pageSize);
        requireOneOf(requestedMaxPages, requestedMaxMemory);
        int bufferAlignment = bufferAlignment(pageSize);
        int maxPages =
                selectMaxPages(requestedMaxPages, requestedMaxMemory, metaDataBytesPerPage, pageSize, bufferAlignment);
        return new GrabAllocator(
                maxPages, metaDataBytesPerPage, pageSize, memoryTracker, preTouch, log, bufferAlignment);
    }

    private static void requireOneOf(Integer requestedMaxPages, Long requestedMaxMemory) {
        if ((requestedMaxPages == null) == (requestedMaxMemory == null)) {
            throw new IllegalArgumentException(
                    "Exactly one of a page count or a memory budget must be given to size the page cache.");
        }
    }

    /**
     * Selects the maximum number of pages to allocate.
     * If a specific number of pages is requested, it returns that value.
     * Otherwise, it calculates the largest number of pages whose required memory fits in the budget.
     */
    private static int selectMaxPages(
            Integer requestedMaxPages,
            Long requestedMaxMemory,
            int metaDataBytesPerPage,
            int pageSize,
            int bufferAlignment) {
        if (requestedMaxPages != null) {
            return requestedMaxPages;
        }
        long overhead = bufferAlignment - 1;
        long bytesPerPage = (long) metaDataBytesPerPage + pageSize;
        long requiredPages = (requestedMaxMemory - overhead) / bytesPerPage;
        if (requiredPages > MAX_PAGES) {
            long maxSupportedBytes = MAX_PAGES * bytesPerPage + overhead;
            throw new IllegalArgumentException(String.format(
                    "Configured page cache size of %s bytes (%s) can not be created. Maximum supported page cache size is %s bytes (%s).",
                    requestedMaxMemory,
                    ByteUnit.bytesToString(requestedMaxMemory),
                    maxSupportedBytes,
                    ByteUnit.bytesToString(maxSupportedBytes)));
        }
        return Math.toIntExact(requiredPages);
    }

    private GrabAllocator(
            int maxPages,
            int metaDataBytesPerPage,
            int pageSize,
            MemoryTracker memoryTracker,
            boolean preTouch,
            Consumer<String> log,
            int bufferAlignment) {
        if (maxPages < MINIMUM_PAGE_COUNT) {
            throw new IllegalArgumentException(format(
                    "Page cache must have at least %s pages, but was given %s pages.", MINIMUM_PAGE_COUNT, maxPages));
        }
        this.maxPages = maxPages;
        this.grabs = new Grabs(maxPages, pageSize, metaDataBytesPerPage, bufferAlignment, preTouch, memoryTracker, log);
        this.cleanable = GLOBAL_CLEANER.register(this, grabs::close);
    }

    /**
     * @return the number of pages this allocator holds buffers and metadata for.
     */
    public int maxPages() {
        return maxPages;
    }

    /**
     * Hand out the next page buffer, sized and aligned as the allocator was configured.
     *
     * @return A pointer to the page buffer.
     * @throws IllegalStateException if all pages have already been handed out.
     */
    public synchronized long allocatePage() {
        return grabs.allocatePage();
    }

    /**
     * @return the pointer to the single 8-byte-aligned metadata region.
     */
    public long metadataAddress() {
        return grabs.metadataAddress();
    }

    /**
     * If memory page size is larger than cache page size, alignment by memory page produces too much memory waste.
     * Therefore, the cache page size is used as the upper bound for alignment.
     */
    public static int bufferAlignment(int pageSize) {
        return Math.min(UnsafeUtil.pageSize(), pageSize);
    }

    @Override
    public void close() {
        cleanable.clean();
    }

    private static Cleaner globalCleaner() {
        return Cleaner.create();
    }
}
