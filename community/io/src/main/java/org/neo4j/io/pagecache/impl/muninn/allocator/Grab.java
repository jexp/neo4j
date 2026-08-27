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

import java.util.function.Consumer;
import org.neo4j.internal.nativeimpl.NativeAccessProvider;
import org.neo4j.internal.nativeimpl.NativeCallResult;
import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.memory.MemoryTracker;

/**
 * Two variants of native memory allocations:
 *  - MetadataGrab - Relies on alignment provided by malloc which guarantees that "the memory is aligned such that it can be used for any data type."
 *  - PageGrab - Enforces requested alignment. Does not touch memory on construction, even if UnsafeUtil#DIRTY_MEMORY is set. Provides a way to touch memory later.
 */
abstract sealed class Grab permits Grab.MetadataGrab, Grab.PageGrab {

    private boolean released = false;

    abstract long base();

    abstract void free(MemoryTracker memoryTracker);

    synchronized void free(long address, long size, MemoryTracker memoryTracker) {
        if (released) {
            return;
        }
        UnsafeUtil.free(address, size, memoryTracker);
        released = true;
    }

    static final class MetadataGrab extends Grab {
        private final long address;
        private final long size;

        MetadataGrab(long size, MemoryTracker memoryTracker) {
            this.address = UnsafeUtil.allocateMemory(size, memoryTracker);
            this.size = size;
        }

        @Override
        long base() {
            return address;
        }

        @Override
        void free(MemoryTracker memoryTracker) {
            free(address, size, memoryTracker);
        }
    }

    static final class PageGrab extends Grab {
        private final long address;
        private final long size;
        private final long base;
        PageGrab next;

        PageGrab(long size, long alignment, MemoryTracker memoryTracker) {
            long toAllocate = size + alignment - 1;
            this.address = UnsafeUtil.allocateUntouchedMemory(toAllocate, memoryTracker);
            this.size = toAllocate;
            this.base = nextAligned(address, alignment);
        }

        private static long nextAligned(long pointer, long alignment) {
            long off = pointer % alignment;
            if (off == 0) {
                return pointer;
            }
            return pointer + (alignment - off);
        }

        @Override
        long base() {
            return base;
        }

        @Override
        void free(MemoryTracker memoryTracker) {
            free(address, size, memoryTracker);
        }

        long size() {
            return size;
        }

        void touch(long offset, long length, Consumer<NativeCallResult> errorHandler) {
            long from = address + offset;
            if (tryPopulateMemory(from, length, errorHandler)) {
                return;
            }
            touchEachPage(from, length);
        }

        private static boolean tryPopulateMemory(long address, long bytes, Consumer<NativeCallResult> errorHandler) {
            var nativeAccess = NativeAccessProvider.getNativeAccess();
            if (!nativeAccess.isAvailable()) {
                return false;
            }
            long alignedAddress = address & -(long) UnsafeUtil.pageSize();
            var result = nativeAccess.tryPopulateMemory(alignedAddress, bytes + (address - alignedAddress));
            if (result.isError()) {
                errorHandler.accept(result);
                return false;
            }
            return true;
        }

        private static void touchEachPage(long address, long bytes) {
            int pageSize = UnsafeUtil.pageSize();
            long end = address + bytes;
            for (long page = address; page < end; page += pageSize) {
                UnsafeUtil.putByte(page, (byte) 0);
            }
        }
    }
}
