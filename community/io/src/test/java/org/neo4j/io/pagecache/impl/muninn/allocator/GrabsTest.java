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
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.neo4j.function.Consumers.ignoreValue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.memory.MemoryLimitExceededException;
import org.neo4j.memory.MemoryPools;

class GrabsTest {
    private static final int PAGE_SIZE = PageCache.PAGE_SIZE;
    private static final int METADATA_SIZE = 32;
    private static final int ALIGNMENT = GrabAllocator.bufferAlignment(PAGE_SIZE);

    private Grabs grabs;

    @AfterEach
    void tearDown() {
        closeGrabs();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void allocatedPageMustBeAligned(boolean preTouch) {
        Grabs grabs = newGrabs(8, preTouch, new LocalMemoryTracker());
        long address = grabs.allocatePage();
        assertThat(address).isNotZero();
        assertThat(address % ALIGNMENT).isZero();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void everyPageMustBeObtainableAndConsecutive(boolean preTouch) {
        int maxPages = 200;
        LocalMemoryTracker memoryTracker = new LocalMemoryTracker();
        Grabs grabs = newGrabs(maxPages, preTouch, memoryTracker);

        long[] pages = new long[maxPages];
        long base = 0;
        for (int i = 0; i < pages.length; i++) {
            long address = grabs.allocatePage();
            if (i == 0) {
                base = address;
            }
            assertThat(address % ALIGNMENT).isZero();
            assertThat(address - base).isEqualTo((long) i * PAGE_SIZE);
            UnsafeUtil.putLong(address, 2L * i);
            UnsafeUtil.putLong(address + PAGE_SIZE - Long.BYTES, 2L * i + 1);
            pages[i] = address;
        }

        assertThat(memoryTracker.usedNativeMemory()).isEqualTo(memoryRequired(maxPages));

        for (int i = 0; i < pages.length; i++) {
            assertThat(UnsafeUtil.getLong(pages[i])).isEqualTo(2L * i);
            assertThat(UnsafeUtil.getLong(pages[i] + PAGE_SIZE - Long.BYTES)).isEqualTo(2L * i + 1);
        }

        closeGrabs();
        assertThat(memoryTracker.usedNativeMemory()).isZero();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void allocationBeyondMaxPagesMustThrow(boolean preTouch) {
        int maxPages = 4;
        Grabs grabs = newGrabs(maxPages, preTouch, new LocalMemoryTracker());
        for (int i = 0; i < maxPages; i++) {
            grabs.allocatePage();
        }
        assertThatExceptionOfType(IllegalStateException.class).isThrownBy(grabs::allocatePage);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void allMemoryMustBeAllocatedUpFront(boolean preTouch) {
        LocalMemoryTracker memoryTracker = new LocalMemoryTracker();
        int maxPages = 512;
        newGrabs(maxPages, preTouch, memoryTracker);

        assertThat(memoryTracker.usedNativeMemory()).isEqualTo(memoryRequired(maxPages));

        closeGrabs();
        assertThat(memoryTracker.usedNativeMemory()).isZero();
    }

    @Test
    void preTouchedPagesMustBeUsable() {
        LocalMemoryTracker memoryTracker = new LocalMemoryTracker();
        int maxPages = 1024;
        Grabs grabs = newGrabs(maxPages, true, memoryTracker);

        long[] pages = new long[maxPages];
        for (int i = 0; i < pages.length; i++) {
            long address = grabs.allocatePage();
            assertThat(address).isNotZero();
            UnsafeUtil.putLong(address, i);
            UnsafeUtil.putLong(address + PAGE_SIZE - Long.BYTES, i);
            pages[i] = address;
        }
        for (int i = 0; i < pages.length; i++) {
            assertThat(UnsafeUtil.getLong(pages[i])).isEqualTo(i);
            assertThat(UnsafeUtil.getLong(pages[i] + PAGE_SIZE - Long.BYTES)).isEqualTo(i);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void failureDuringConstructionMustReleaseAllMemory(boolean preTouch) {
        var memoryTracker = new LocalMemoryTracker(
                MemoryPools.NO_TRACKING, ByteUnit.mebiBytes(2) + ByteUnit.kibiBytes(512), 0, "test-limit");
        assertThatExceptionOfType(MemoryLimitExceededException.class)
                .isThrownBy(() -> new Grabs.PreAllocated(
                        (int) (ByteUnit.mebiBytes(4) / PAGE_SIZE),
                        PAGE_SIZE,
                        ByteUnit.kibiBytes(64),
                        ALIGNMENT,
                        preTouch,
                        memoryTracker,
                        ignoreValue()));
        assertThat(memoryTracker.usedNativeMemory()).isZero();
    }

    @Test
    void lazyGrabsMustAllocatePageBuffersOnDemand() {
        int maxPages = Grabs.Lazy.pagesPerGrab(PAGE_SIZE) * 3;
        LocalMemoryTracker memoryTracker = new LocalMemoryTracker();
        grabs = new Grabs.Lazy(maxPages, PAGE_SIZE, METADATA_SIZE, ALIGNMENT, memoryTracker);

        long metadataOnly = (long) maxPages * METADATA_SIZE;
        assertThat(memoryTracker.usedNativeMemory()).isEqualTo(metadataOnly);

        long[] pages = new long[maxPages];
        for (int i = 0; i < pages.length; i++) {
            long address = grabs.allocatePage();
            assertThat(address % ALIGNMENT).isZero();
            UnsafeUtil.putLong(address, 2L * i);
            UnsafeUtil.putLong(address + PAGE_SIZE - Long.BYTES, 2L * i + 1);
            pages[i] = address;
            assertThat(memoryTracker.usedNativeMemory()).isGreaterThan(metadataOnly);
        }
        assertThatExceptionOfType(IllegalStateException.class).isThrownBy(grabs::allocatePage);

        for (int i = 0; i < pages.length; i++) {
            assertThat(UnsafeUtil.getLong(pages[i])).isEqualTo(2L * i);
            assertThat(UnsafeUtil.getLong(pages[i] + PAGE_SIZE - Long.BYTES)).isEqualTo(2L * i + 1);
        }
        assertThat(memoryTracker.usedNativeMemory()).isEqualTo(memoryRequired(maxPages) + 2L * (ALIGNMENT - 1));

        closeGrabs();
        assertThat(memoryTracker.usedNativeMemory()).isZero();
    }

    @Test
    void lazyGrabsMustNotAllocateMoreThanMaxPages() {
        int pagesPerGrab = Grabs.Lazy.pagesPerGrab(PAGE_SIZE);
        int maxPages = pagesPerGrab + 1;
        LocalMemoryTracker memoryTracker = new LocalMemoryTracker();
        grabs = new Grabs.Lazy(maxPages, PAGE_SIZE, METADATA_SIZE, ALIGNMENT, memoryTracker);

        for (int i = 0; i < maxPages; i++) {
            grabs.allocatePage();
        }

        long expected = (long) maxPages * (METADATA_SIZE + PAGE_SIZE) + 2L * (ALIGNMENT - 1);
        assertThat(memoryTracker.usedNativeMemory()).isEqualTo(expected);
    }

    private void closeGrabs() {
        if (grabs != null) {
            grabs.close();
            grabs = null;
        }
    }

    private Grabs newGrabs(int maxPages, boolean preTouch, LocalMemoryTracker memoryTracker) {
        closeGrabs();
        grabs = new Grabs.PreAllocated(
                maxPages, PAGE_SIZE, METADATA_SIZE, ALIGNMENT, preTouch, memoryTracker, ignoreValue());
        return grabs;
    }

    private static long memoryRequired(int maxPages) {
        return maxPages * ((long) METADATA_SIZE + PAGE_SIZE) + ALIGNMENT - 1;
    }
}
