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

import static java.lang.String.format;
import static org.neo4j.function.Consumers.ignoreValue;

import java.io.BufferedWriter;
import java.io.IOException;
import java.lang.invoke.VarHandle;
import java.util.function.Consumer;
import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.impl.muninn.allocator.GrabAllocator;
import org.neo4j.io.pagecache.tracing.PageReferenceTranslator;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.VisibleForTesting;

/**
 * The PageMetadata maintains metadata for the individual memory pages and provides static methods for working with it.
 * Metadata for all pages is stored in a contiguous block of off-heap memory and could be considered as a single array.
 *
 * There are few page-* terms used in page cache implementation, and metadata connects them:
 * - pageId - is an identifier for a specific page in the page cache, it is also index in the metadata array; they go from 0 to `pageCount` - 1.
 * - pageRef - is a direct pointer to the metadata for the specific page, obtained by calling `deref` method on this class.
 * - filePageId - number of the page in the specific file; it is written into the metadata if page cache page is bound to a file page.
 *
 * The metadata for each page is the following:
 *
 * Bytes | Usage
 * 8     | Sequence lock word descibed by {@link OffHeapPageLock}.
 * 8     | Pointer to the memory page - actual 8K of memory that holds the page data.
 * 8     | Last modified transaction id, or previous chain version if multiversioned page
 * 8     | Page binding.
 *
 * Page binding is a 64-bit word that has the following format:
 *
 *    ┏━ File page id (40 bits)                         ┏━ Swapper id (21 bits) ┏━ Page usage counter (3 bits)
 * ┏━━┻━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓ ┏━━┻━━━━━━━━━━━━━━━━━━━━━┓┏┻┓
 * PPPP PPPP PPPP PPPP PPPP PPPP PPPP PPPP PPPP PPPP SSSS SSSS SSSS SSSS SSSS SRRR
 */
class PageMetadata implements PageReferenceTranslator, AutoCloseable {

    static final int META_DATA_BYTES_PER_PAGE = 32;

    private static final long UNBOUND_LAST_MODIFIED_TX_ID = 0;
    private static final long MAX_USAGE_COUNT = 4;
    private static final int SHIFT_FILE_PAGE_ID = 24;
    private static final int SHIFT_SWAPPER_ID = 3;
    private static final int SHIFT_PARTIAL_FILE_PAGE_ID = SHIFT_FILE_PAGE_ID - SHIFT_SWAPPER_ID;
    private static final long MASK_USAGE_COUNT = (1L << SHIFT_SWAPPER_ID) - 1L;
    private static final long MASK_NOT_FILE_PAGE_ID = (1L << SHIFT_FILE_PAGE_ID) - 1L;
    private static final long MASK_SHIFTED_SWAPPER_ID = MASK_NOT_FILE_PAGE_ID >>> SHIFT_SWAPPER_ID;
    private static final long MASK_NOT_SWAPPER_ID = ~(MASK_SHIFTED_SWAPPER_ID << SHIFT_SWAPPER_ID);
    private static final long UNBOUND_PAGE_BINDING = PageCursor.UNBOUND_PAGE_ID << SHIFT_FILE_PAGE_ID;

    // 40 bits for file page id
    private static final long MAX_FILE_PAGE_ID = (1L << Long.SIZE - SHIFT_FILE_PAGE_ID) - 1L;

    private static final int OFFSET_LOCK_WORD = 0; // 8 bytes.
    private static final int OFFSET_ADDRESS = 8; // 8 bytes.
    private static final int OFFSET_LAST_TX_ID = 16; // 8 bytes.
    // we use the same bytes to store previous chain version as
    // single version OFFSET_LAST_TX_ID, since those should never work together
    private static final int OFFSET_PREVIOUS_CHAIN_TX_ID = OFFSET_LAST_TX_ID;
    // The high 5 bytes of the page binding are the file page id.
    // The 21 following lower bits are the swapper id.
    // And the last 3 low bits are the usage counter.
    private static final int OFFSET_PAGE_BINDING = 24; // 8 bytes.
    // UNKNOWN value of previous chain modifier. Page with this modifier is always flushable.
    private static final int UNKNOWN_CHAIN_MODIFIER = 0;

    private final GrabAllocator memoryAllocator;
    private final int pageCount;
    private final long baseAddress;

    PageMetadata(
            Integer requestedMaxPages,
            Long requestedMaxMemory,
            int pageSize,
            MemoryTracker memoryTracker,
            boolean preTouch,
            Consumer<String> log) {
        this.memoryAllocator = GrabAllocator.createAllocator(
                requestedMaxPages,
                requestedMaxMemory,
                META_DATA_BYTES_PER_PAGE,
                pageSize,
                memoryTracker,
                preTouch,
                log);
        this.pageCount = memoryAllocator.maxPages();
        this.baseAddress = memoryAllocator.metadataAddress();
        clearMemory(baseAddress, pageCount);
    }

    @VisibleForTesting
    PageMetadata(int pageCount, int pageSize) {
        this(pageCount, null, pageSize, EmptyMemoryTracker.INSTANCE, false, ignoreValue());
    }

    long ensurePageAllocated(long pageRef) {
        long address = getAddress(pageRef);
        if (address != 0L) {
            return address;
        }
        long allocated = memoryAllocator.allocatePage();
        setAddress(pageRef, allocated);
        return allocated;
    }

    @Override
    public void close() {
        memoryAllocator.close();
    }

    /**
     * @return The capacity of the page list.
     */
    int getPageCount() {
        return pageCount;
    }

    /**
     * Turn a {@code pageId} into a {@code pageRef} that can be used for accessing and manipulating the given page
     * using the other methods in this class.
     *
     * @param pageId The {@code pageId} to turn into a {@code pageRef}.
     * @return A {@code pageRef} which is an opaque, internal and direct pointer to the meta-data of the given memory
     * page.
     */
    long deref(int pageId) {
        assert pageId >= 0 && pageId < pageCount : "PageId out of range: " + pageId + ". PageCount: " + pageCount;
        //noinspection UnnecessaryLocalVariable
        long id = pageId; // convert to long to avoid int multiplication
        return baseAddress + (id * META_DATA_BYTES_PER_PAGE);
    }

    /**
     * Turn a {@code pageRef} into a page cache {@code pageId}
     */
    @Override
    public int toId(long pageRef) {
        // >> 5 is equivalent to dividing by 32, META_DATA_BYTES_PER_PAGE.
        return (int) ((pageRef - baseAddress) >> 5);
    }

    private static void clearMemory(long baseAddress, long pageCount) {
        long memcpyChunkSize = UnsafeUtil.pageSize();
        long metaDataEntriesPerChunk = memcpyChunkSize / META_DATA_BYTES_PER_PAGE;
        if (pageCount < metaDataEntriesPerChunk) {
            clearMemorySimple(baseAddress, pageCount);
        } else {
            clearMemoryFast(baseAddress, pageCount, memcpyChunkSize, metaDataEntriesPerChunk);
        }
        VarHandle.fullFence(); // Guarantee the visibility of the cleared memory.
    }

    private static void clearMemorySimple(long baseAddress, long pageCount) {
        long address = baseAddress - Long.BYTES;
        long initialLockWord = OffHeapPageLock.initialLockWordWithExclusiveLock();
        for (long i = 0; i < pageCount; i++) {
            UnsafeUtil.putLong(address += Long.BYTES, initialLockWord); // lock word
            UnsafeUtil.putLong(address += Long.BYTES, 0); // pointer
            UnsafeUtil.putLong(address += Long.BYTES, 0); // last tx id
            UnsafeUtil.putLong(address += Long.BYTES, UNBOUND_PAGE_BINDING);
        }
    }

    private static void clearMemoryFast(
            long baseAddress, long pageCount, long memcpyChunkSize, long metaDataEntriesPerChunk) {
        // Initialise one chunk worth of data.
        clearMemorySimple(baseAddress, metaDataEntriesPerChunk);
        // Since all entries contain the same data, we can now copy this chunk over and over.
        long chunkCopies = pageCount / metaDataEntriesPerChunk - 1;
        long address = baseAddress + memcpyChunkSize;
        for (int i = 0; i < chunkCopies; i++) {
            UnsafeUtil.copyMemory(baseAddress, address, memcpyChunkSize);
            address += memcpyChunkSize;
        }
        // Finally fill in the tail.
        long tailCount = pageCount % metaDataEntriesPerChunk;
        clearMemorySimple(address, tailCount);
    }

    private static long offsetLastModifiedTransactionId(long pageRef) {
        return pageRef + OFFSET_LAST_TX_ID;
    }

    private static long offsetPageHorizon(long pageRef) {
        return pageRef + OFFSET_PREVIOUS_CHAIN_TX_ID;
    }

    private static long offsetLock(long pageRef) {
        return pageRef + OFFSET_LOCK_WORD;
    }

    private static long offsetAddress(long pageRef) {
        return pageRef + OFFSET_ADDRESS;
    }

    private static long offsetPageBinding(long pageRef) {
        return pageRef + OFFSET_PAGE_BINDING;
    }

    static long tryOptimisticReadLock(long pageRef) {
        return OffHeapPageLock.tryOptimisticReadLock(offsetLock(pageRef));
    }

    static boolean validateReadLock(long pageRef, long stamp) {
        return OffHeapPageLock.validateReadLock(offsetLock(pageRef), stamp);
    }

    static boolean isModified(long pageRef) {
        return OffHeapPageLock.isModified(offsetLock(pageRef));
    }

    static boolean isExclusivelyLocked(long pageRef) {
        return OffHeapPageLock.isExclusivelyLocked(offsetLock(pageRef));
    }

    static boolean isWriteLocked(long pageRef) {
        return OffHeapPageLock.isWriteLocked(offsetLock(pageRef));
    }

    static boolean tryWriteLock(long pageRef, boolean singleWriter) {
        return OffHeapPageLock.tryWriteLock(offsetLock(pageRef), singleWriter);
    }

    static void unlockWrite(long pageRef) {
        OffHeapPageLock.unlockWrite(offsetLock(pageRef));
    }

    static long unlockWriteAndTryTakeFlushLock(long pageRef) {
        return OffHeapPageLock.unlockWriteAndTryTakeFlushLock(offsetLock(pageRef));
    }

    static boolean tryExclusiveLock(long pageRef) {
        return OffHeapPageLock.tryExclusiveLock(offsetLock(pageRef));
    }

    static long unlockExclusive(long pageRef) {
        return OffHeapPageLock.unlockExclusive(offsetLock(pageRef));
    }

    static void unlockExclusiveAndTakeWriteLock(long pageRef) {
        OffHeapPageLock.unlockExclusiveAndTakeWriteLock(offsetLock(pageRef));
    }

    static long tryFlushLock(long pageRef) {
        return OffHeapPageLock.tryFlushLock(offsetLock(pageRef));
    }

    static void unlockFlush(long pageRef, long stamp, boolean success) {
        OffHeapPageLock.unlockFlush(offsetLock(pageRef), stamp, success);
    }

    static void explicitlyMarkPageUnmodifiedUnderExclusiveLock(long pageRef) {
        OffHeapPageLock.explicitlyMarkPageUnmodifiedUnderExclusiveLock(offsetLock(pageRef));
    }

    static long getAddress(long pageRef) {
        return UnsafeUtil.getLong(offsetAddress(pageRef));
    }

    static void setAddress(long pageRef, long address) {
        UnsafeUtil.putLong(offsetAddress(pageRef), address);
    }

    /**
     * Increment the usage stamp to at most 4.
     **/
    static void incrementUsage(long pageRef) {
        // This is intentionally left benignly racy for performance.
        long address = offsetPageBinding(pageRef);
        long value = UnsafeUtil.getLongVolatile(address);
        long usage = value & MASK_USAGE_COUNT;
        if (usage < MAX_USAGE_COUNT) // avoid cache sloshing by not doing a write if counter is already maxed out
        {
            long update = value + 1;
            // Use compareAndSwapLong to only actually store the updated count if nothing else changed
            // in this word-line. The word-line is shared with the file page id, and the swapper id.
            // Those fields are updated under guard of the exclusive lock, but we *might* race with
            // that here, and in that case we would never want a usage counter update to clobber a page
            // binding update.
            UnsafeUtil.compareAndSwapLong(null, address, value, update);
        }
    }

    /**
     * Decrement the usage stamp. Returns true if it reaches 0.
     **/
    static boolean decrementUsage(long pageRef) {
        // This is intentionally left benignly racy for performance.
        long address = offsetPageBinding(pageRef);
        long value = UnsafeUtil.getLongVolatile(address);
        long usage = value & MASK_USAGE_COUNT;
        if (usage > 0) {
            long update = value - 1;
            // See `incrementUsage` about why we use `compareAndSwapLong`.
            UnsafeUtil.compareAndSwapLong(null, address, value, update);
        }
        return usage <= 1;
    }

    static long getUsage(long pageRef) {
        return UnsafeUtil.getLongVolatile(offsetPageBinding(pageRef)) & MASK_USAGE_COUNT;
    }

    static long getFilePageId(long pageRef) {
        long filePageId = UnsafeUtil.getLong(offsetPageBinding(pageRef)) >>> SHIFT_FILE_PAGE_ID;
        return filePageId == MAX_FILE_PAGE_ID ? PageCursor.UNBOUND_PAGE_ID : filePageId;
    }

    static void setFilePageId(long pageRef, long filePageId) {
        if (filePageId > MAX_FILE_PAGE_ID) {
            throw new IllegalArgumentException(
                    format("File page id: %s is bigger then max supported value %s.", filePageId, MAX_FILE_PAGE_ID));
        }
        long address = offsetPageBinding(pageRef);
        long v = UnsafeUtil.getLong(address);
        filePageId = (filePageId << SHIFT_FILE_PAGE_ID) + (v & MASK_NOT_FILE_PAGE_ID);
        UnsafeUtil.putLong(address, filePageId);
    }

    static long getLastModifiedTxId(long pageRef) {
        return UnsafeUtil.getLongVolatile(offsetLastModifiedTransactionId(pageRef));
    }

    /**
     * @return return last modifier transaction id and resets it to {@link #UNBOUND_LAST_MODIFIED_TX_ID}
     */
    static long getAndResetLastModifiedTransactionId(long pageRef) {
        return UnsafeUtil.getAndSetLong(null, offsetLastModifiedTransactionId(pageRef), UNBOUND_LAST_MODIFIED_TX_ID);
    }

    static void setLastModifiedTxId(long pageRef, long modifierTxId) {
        UnsafeUtil.compareAndSetMaxLong(null, offsetLastModifiedTransactionId(pageRef), modifierTxId);
    }

    static long getAndResetPageHorizon(long pageRef) {
        return UnsafeUtil.getAndSetLong(null, offsetPageHorizon(pageRef), UNKNOWN_CHAIN_MODIFIER);
    }

    static long getPageHorizon(long pageRef) {
        return UnsafeUtil.getLongVolatile(offsetPageHorizon(pageRef));
    }

    static void setPageHorizon(long pageRef, long horizon) {
        UnsafeUtil.putLong(offsetPageHorizon(pageRef), horizon);
    }

    static int getSwapperId(long pageRef) {
        long v = UnsafeUtil.getLong(offsetPageBinding(pageRef)) >>> SHIFT_SWAPPER_ID;
        return (int) (v & MASK_SHIFTED_SWAPPER_ID); // 21 bits.
    }

    static void setSwapperId(long pageRef, int swapperId) {
        swapperId = swapperId << SHIFT_SWAPPER_ID;
        long address = offsetPageBinding(pageRef);
        long v = UnsafeUtil.getLong(address) & MASK_NOT_SWAPPER_ID;
        UnsafeUtil.putLong(address, v + swapperId);
    }

    static boolean isLoaded(long pageRef) {
        return getFilePageId(pageRef) != PageCursor.UNBOUND_PAGE_ID;
    }

    static boolean isBoundTo(long pageRef, int swapperId, long filePageId) {
        long address = offsetPageBinding(pageRef);
        long expectedBinding = (filePageId << SHIFT_PARTIAL_FILE_PAGE_ID) + swapperId;
        long actualBinding = UnsafeUtil.getLong(address) >>> SHIFT_SWAPPER_ID;
        return expectedBinding == actualBinding;
    }

    static void clearBinding(long pageRef) {
        PageMetadata.getAndResetPageHorizon(pageRef);
        UnsafeUtil.putLong(offsetPageBinding(pageRef), UNBOUND_PAGE_BINDING);
    }

    public static String pageMetadata(long pageRef) {
        return "Lock word: " + Long.toHexString(UnsafeUtil.getLong(offsetLock(pageRef))) + "\nAddress: "
                + Long.toHexString(UnsafeUtil.getLong(offsetAddress(pageRef))) + "\nPrevious/Last TxId: "
                + Long.toHexString(UnsafeUtil.getLong(offsetPageHorizon(pageRef))) + "\nBinding: "
                + Long.toHexString(UnsafeUtil.getLong(offsetPageBinding(pageRef)));
    }

    public void dump(BufferedWriter writer) throws IOException {
        for (int i = 0; i < pageCount; i++) {
            writer.write("Page : " + i);
            writer.newLine();
            writer.write(pageMetadata(deref(i)));
            writer.newLine();
        }
    }
}
