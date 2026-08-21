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

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.function.Consumers.ignoreValue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.memory.LocalMemoryTracker;

class GrabAllocatorTest {
    private static final int PAGE_SIZE = PageCache.PAGE_SIZE;
    private static final int META = 32;

    private GrabAllocator allocator;

    @AfterEach
    void tearDown() {
        closeAllocator();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void allocatedPageMustNotBeNullAndMustBeAligned(boolean preTouch) {
        GrabAllocator mman = createAllocator(8, PAGE_SIZE, preTouch);
        long address = mman.allocatePage();
        assertThat(address).isNotZero();
        assertThat(address % GrabAllocator.bufferAlignment(PAGE_SIZE)).isZero();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void allocatedMetadataMustBeEightByteAligned(boolean preTouch) {
        GrabAllocator mman = createAllocator(8, PAGE_SIZE, preTouch);
        long address = mman.metadataAddress();
        assertThat(address).isNotZero();
        assertThat(address % Long.BYTES).isZero();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void metadataAndEveryPageMustBeObtainableAndStayWithinTheRequiredMemory(boolean preTouch) {
        int maxPages = 200;
        LocalMemoryTracker memoryTracker = new LocalMemoryTracker();
        GrabAllocator mman = allocator =
                GrabAllocator.createAllocator(maxPages, null, META, PAGE_SIZE, memoryTracker, preTouch, ignoreValue());

        long metadataBytes = (long) maxPages * META;
        long region = mman.metadataAddress();
        assertThat(region % Long.BYTES).isZero();
        UnsafeUtil.putLong(region, Long.MIN_VALUE + 1);
        UnsafeUtil.putLong(region + metadataBytes - Long.BYTES, Long.MIN_VALUE + 2);

        long[] pages = new long[maxPages];
        for (int i = 0; i < pages.length; i++) {
            long address = mman.allocatePage();
            assertThat(address % GrabAllocator.bufferAlignment(PAGE_SIZE)).isZero();
            UnsafeUtil.putLong(address, 2L * i);
            UnsafeUtil.putLong(address + PAGE_SIZE - Long.BYTES, 2L * i + 1);
            pages[i] = address;
        }

        assertThat(memoryTracker.usedNativeMemory())
                .isEqualTo(memoryRequired(maxPages, META, PAGE_SIZE, GrabAllocator.bufferAlignment(PAGE_SIZE)));

        assertThat(UnsafeUtil.getLong(region)).isEqualTo(Long.MIN_VALUE + 1);
        assertThat(UnsafeUtil.getLong(region + metadataBytes - Long.BYTES)).isEqualTo(Long.MIN_VALUE + 2);
        for (int i = 0; i < pages.length; i++) {
            assertThat(UnsafeUtil.getLong(pages[i])).isEqualTo(2L * i);
            assertThat(UnsafeUtil.getLong(pages[i] + PAGE_SIZE - Long.BYTES)).isEqualTo(2L * i + 1);
        }

        closeAllocator();
        assertThat(memoryTracker.usedNativeMemory()).isZero();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void trackMemoryAllocations(boolean preTouch) {
        LocalMemoryTracker memoryTracker = new LocalMemoryTracker();
        int maxPages = 4;
        allocator =
                GrabAllocator.createAllocator(maxPages, null, META, PAGE_SIZE, memoryTracker, preTouch, ignoreValue());

        long expected = memoryRequired(maxPages, META, PAGE_SIZE, GrabAllocator.bufferAlignment(PAGE_SIZE));
        assertThat(memoryTracker.usedNativeMemory()).isEqualTo(expected);
        for (int i = 0; i < maxPages; i++) {
            allocator.allocatePage();
        }
        assertThat(memoryTracker.usedNativeMemory()).isEqualTo(expected);

        allocator.close();
        allocator = null;
        assertThat(memoryTracker.usedNativeMemory()).isZero();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void largePagesMustBeObtainableAndUsable(boolean preTouch) {
        int pageSize = (int) ByteUnit.mebiBytes(4);
        long budget = Math.max(ByteUnit.mebiBytes(8), 10L * pageSize);
        LocalMemoryTracker memoryTracker = new LocalMemoryTracker();
        allocator = GrabAllocator.createAllocator(null, budget, META, pageSize, memoryTracker, preTouch, ignoreValue());

        int maxPages = allocator.maxPages();
        assertThat(maxPages).isGreaterThanOrEqualTo(2);
        for (int i = 0; i < maxPages; i++) {
            long address = allocator.allocatePage();
            assertThat(address % GrabAllocator.bufferAlignment(pageSize)).isZero();
        }
        assertThat(memoryTracker.usedNativeMemory()).isLessThanOrEqualTo(budget);
    }

    private void closeAllocator() {
        if (allocator != null) {
            allocator.close();
            allocator = null;
        }
    }

    private GrabAllocator createAllocator(int maxPages, int pageSize, boolean preTouch) {
        closeAllocator();
        allocator = GrabAllocator.createAllocator(
                maxPages, null, META, pageSize, new LocalMemoryTracker(), preTouch, ignoreValue());
        return allocator;
    }

    public static long memoryRequired(int maxPages, int metaDataBytesPerPage, int pageSize, int bufferAlignment) {
        return maxPages * ((long) metaDataBytesPerPage + pageSize) + bufferAlignment - 1;
    }
}
