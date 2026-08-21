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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntFunction;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.io.pagecache.impl.muninn.swapper.PageSwapper;
import org.neo4j.io.pagecache.tracing.DummyPageSwapper;
import org.neo4j.io.pagecache.tracing.EvictionEvent;
import org.neo4j.io.pagecache.tracing.EvictionEventOpportunity;
import org.neo4j.io.pagecache.tracing.EvictionRunEvent;
import org.neo4j.io.pagecache.tracing.FlushEvent;
import org.neo4j.io.pagecache.tracing.PageReferenceTranslator;
import org.neo4j.io.pagecache.tracing.PinPageFaultEvent;
import org.neo4j.io.pagecache.tracing.async.AsyncEvictionEvent;

public class EvictionLogicTest {

    private static final int[] pageIds = new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    private static final DummyPageSwapper DUMMY_SWAPPER = new DummyPageSwapper("", UnsafeUtil.pageSize());

    private static Stream<Arguments> argumentsProvider() {
        IntFunction<Arguments> toArguments = Arguments::of;
        return Arrays.stream(pageIds).mapToObj(toArguments);
    }

    @AfterEach
    void tearDown() {
        if (pageMetadata != null) {
            pageMetadata.close();
            pageMetadata = null;
        }
    }

    private long pageRef;
    private long prevPageRef;
    private long nextPageRef;
    private int pageSize;
    private SwapperSet swappers;
    private PageMetadata pageMetadata;

    protected void init(int pageId) {
        int prevPageId = pageId == 0 ? pageIds.length - 1 : (pageId - 1) % pageIds.length;
        int nextPageId = (pageId + 1) % pageIds.length;
        pageSize = UnsafeUtil.pageSize();

        swappers = new SwapperSet();
        pageMetadata = new PageMetadata(pageIds.length, pageSize);
        pageRef = pageMetadata.deref(pageId);
        prevPageRef = pageMetadata.deref(prevPageId);
        nextPageRef = pageMetadata.deref(nextPageId);
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void tryEvictMustFailIfPageIsAlreadyExclusivelyLocked(int pageId) throws Exception {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        int swapperId = swappers.allocate(DUMMY_SWAPPER);
        doFault(swapperId, 42); // page is now loaded
        // pages are delivered from the fault routine with the exclusive lock already held!
        assertFalse(EvictionLogic.tryEvict(pageRef, EvictionRunEvent.NULL, swappers, pageMetadata));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void tryEvictThatFailsOnExclusiveLockMustNotUndoSaidLock(int pageId) throws Exception {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        int swapperId = swappers.allocate(DUMMY_SWAPPER);
        doFault(swapperId, 42); // page is now loaded
        // pages are delivered from the fault routine with the exclusive lock already held!
        EvictionLogic.tryEvict(pageRef, EvictionRunEvent.NULL, swappers, pageMetadata); // This attempt will fail
        assertTrue(PageMetadata.isExclusivelyLocked(pageRef)); // page should still have its lock
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void tryEvictMustFailIfPageIsNotLoaded(int pageId) throws Exception {
        init(pageId);

        assertFalse(EvictionLogic.tryEvict(pageRef, EvictionRunEvent.NULL, swappers, pageMetadata));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void tryEvictMustWhenPageIsNotLoadedMustNotLeavePageLocked(int pageId) throws Exception {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        EvictionLogic.tryEvict(pageRef, EvictionRunEvent.NULL, swappers, pageMetadata); // This attempt fails
        assertFalse(PageMetadata.isExclusivelyLocked(pageRef)); // Page should not be left in locked state
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void tryEvictMustLeavePageExclusivelyLockedOnSuccess(int pageId) throws Exception {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        int swapperId = swappers.allocate(DUMMY_SWAPPER);
        doFault(swapperId, 42); // page now bound & exclusively locked
        PageMetadata.unlockExclusive(pageRef); // no longer exclusively locked; can now be evicted
        assertTrue(EvictionLogic.tryEvict(pageRef, EvictionRunEvent.NULL, swappers, pageMetadata));
        PageMetadata.unlockExclusive(pageRef); // will throw if lock is not held
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void pageMustNotBeLoadedAfterSuccessfulEviction(int pageId) throws Exception {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        int swapperId = swappers.allocate(DUMMY_SWAPPER);
        doFault(swapperId, 42); // page now bound & exclusively locked
        PageMetadata.unlockExclusive(pageRef); // no longer exclusively locked; can now be evicted
        assertTrue(PageMetadata.isLoaded(pageRef));
        EvictionLogic.tryEvict(pageRef, EvictionRunEvent.NULL, swappers, pageMetadata);
        assertFalse(PageMetadata.isLoaded(pageRef));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void pageMustNotBeBoundAfterSuccessfulEviction(int pageId) throws Exception {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        int swapperId = swappers.allocate(DUMMY_SWAPPER);
        doFault(swapperId, 42); // page now bound & exclusively locked
        PageMetadata.unlockExclusive(pageRef); // no longer exclusively locked; can now be evicted
        assertTrue(PageMetadata.isBoundTo(pageRef, (short) 1, 42));
        assertTrue(PageMetadata.isLoaded(pageRef));
        assertThat(PageMetadata.getSwapperId(pageRef)).isEqualTo(1);
        EvictionLogic.tryEvict(pageRef, EvictionRunEvent.NULL, swappers, pageMetadata);
        assertFalse(PageMetadata.isBoundTo(pageRef, (short) 1, 42));
        assertFalse(PageMetadata.isLoaded(pageRef));
        assertThat(PageMetadata.getSwapperId(pageRef)).isEqualTo(0);
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void pageMustNotBeModifiedAfterSuccessfulEviction(int pageId) throws Exception {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        int swapperId = swappers.allocate(DUMMY_SWAPPER);
        doFault(swapperId, 42);
        PageMetadata.unlockExclusiveAndTakeWriteLock(pageRef);
        PageMetadata.unlockWrite(pageRef); // page is now modified
        assertTrue(PageMetadata.isModified(pageRef));
        assertTrue(EvictionLogic.tryEvict(pageRef, EvictionRunEvent.NULL, swappers, pageMetadata));
        assertFalse(PageMetadata.isModified(pageRef));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void tryEvictMustFlushPageIfModified(int pageId) throws Exception {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        AtomicLong writtenFilePageId = new AtomicLong(-1);
        AtomicLong writtenBufferAddress = new AtomicLong(-1);
        PageSwapper swapper = new DummyPageSwapper("file", pageSize) {
            @Override
            public long write(long filePageId, long bufferAddress) throws IOException {
                assertTrue(writtenFilePageId.compareAndSet(-1, filePageId));
                assertTrue(writtenBufferAddress.compareAndSet(-1, bufferAddress));
                return super.write(filePageId, bufferAddress);
            }
        };
        int swapperId = swappers.allocate(swapper);
        doFault(swapperId, 42);
        PageMetadata.unlockExclusiveAndTakeWriteLock(pageRef);
        PageMetadata.unlockWrite(pageRef); // page is now modified
        assertTrue(PageMetadata.isModified(pageRef));
        assertTrue(EvictionLogic.tryEvict(pageRef, EvictionRunEvent.NULL, swappers, pageMetadata));
        assertThat(writtenFilePageId.get()).isEqualTo(42L);
        assertThat(writtenBufferAddress.get()).isEqualTo(PageMetadata.getAddress(pageRef));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void tryEvictMustNotFlushPageIfNotModified(int pageId) throws Exception {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        AtomicInteger writes = new AtomicInteger();
        PageSwapper swapper = new DummyPageSwapper("a", 313) {
            @Override
            public long write(long filePageId, long bufferAddress) throws IOException {
                writes.getAndIncrement();
                return super.write(filePageId, bufferAddress);
            }
        };
        int swapperId = swappers.allocate(swapper);
        doFault(swapperId, 42);
        PageMetadata.unlockExclusive(pageRef); // we take no write lock, so page is not modified
        assertFalse(PageMetadata.isModified(pageRef));
        assertTrue(EvictionLogic.tryEvict(pageRef, EvictionRunEvent.NULL, swappers, pageMetadata));
        assertThat(writes.get()).isEqualTo(0);
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void tryEvictMustNotifySwapperOnSuccess(int pageId) throws Exception {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        AtomicBoolean evictionNotified = new AtomicBoolean();
        PageSwapper swapper = new DummyPageSwapper("a", 313) {
            @Override
            public void evicted(long pageRef, long filePageId) {
                evictionNotified.set(true);
                assertThat(filePageId).isEqualTo(42L);
            }
        };
        int swapperId = swappers.allocate(swapper);
        doFault(swapperId, 42);
        PageMetadata.unlockExclusive(pageRef);
        assertTrue(EvictionLogic.tryEvict(pageRef, EvictionRunEvent.NULL, swappers, pageMetadata));
        assertTrue(evictionNotified.get());
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void tryEvictMustNotifySwapperOnSuccessEvenWhenFlushing(int pageId) throws Exception {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        AtomicBoolean evictionNotified = new AtomicBoolean();
        PageSwapper swapper = new DummyPageSwapper("a", 313) {
            @Override
            public void evicted(long pageRef, long filePageId) {
                evictionNotified.set(true);
                assertThat(filePageId).isEqualTo(42L);
            }
        };
        int swapperId = swappers.allocate(swapper);
        doFault(swapperId, 42);
        PageMetadata.unlockExclusiveAndTakeWriteLock(pageRef);
        PageMetadata.unlockWrite(pageRef); // page is now modified
        assertTrue(PageMetadata.isModified(pageRef));
        assertTrue(EvictionLogic.tryEvict(pageRef, EvictionRunEvent.NULL, swappers, pageMetadata));
        assertTrue(evictionNotified.get());
        assertFalse(PageMetadata.isModified(pageRef));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void tryEvictMustLeavePageUnlockedAndLoadedAndBoundAndModifiedIfFlushThrows(int pageId) throws Exception {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        PageSwapper swapper = new DummyPageSwapper("a", 313) {
            @Override
            public long write(long filePageId, long bufferAddress) throws IOException {
                throw new IOException();
            }
        };
        int swapperId = swappers.allocate(swapper);
        doFault(swapperId, 42);
        PageMetadata.unlockExclusiveAndTakeWriteLock(pageRef);
        PageMetadata.unlockWrite(pageRef); // page is now modified
        assertTrue(PageMetadata.isModified(pageRef));
        assertThatThrownBy(() -> EvictionLogic.tryEvict(pageRef, EvictionRunEvent.NULL, swappers, pageMetadata))
                .isInstanceOf(IOException.class);
        // there should be no lock preventing us from taking an exclusive lock
        assertTrue(PageMetadata.tryExclusiveLock(pageRef));
        // page should still be loaded...
        assertTrue(PageMetadata.isLoaded(pageRef));
        // ... and bound
        assertTrue(PageMetadata.isBoundTo(pageRef, swapperId, 42));
        // ... and modified
        assertTrue(PageMetadata.isModified(pageRef));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void tryEvictMustNotNotifySwapperOfEvictionIfFlushThrows(int pageId) throws Exception {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        AtomicBoolean evictionNotified = new AtomicBoolean();
        PageSwapper swapper = new DummyPageSwapper("a", 313) {
            @Override
            public long write(long filePageId, long bufferAddress) throws IOException {
                throw new IOException();
            }

            @Override
            public void evicted(long pageRef, long filePageId) {
                evictionNotified.set(true);
            }
        };
        int swapperId = swappers.allocate(swapper);
        doFault(swapperId, 42);
        PageMetadata.unlockExclusiveAndTakeWriteLock(pageRef);
        PageMetadata.unlockWrite(pageRef); // page is now modified
        assertTrue(PageMetadata.isModified(pageRef));
        assertThatThrownBy(() -> EvictionLogic.tryEvict(pageRef, EvictionRunEvent.NULL, swappers, pageMetadata))
                .isInstanceOf(IOException.class);
        // we should not have gotten any notification about eviction
        assertFalse(evictionNotified.get());
    }

    private static class EvictionRecorderEvent implements EvictionEvent {
        private long filePageId;
        private PageSwapper swapper;
        private IOException evictionException;
        private final long cachePageId;
        private boolean evictionClosed;
        private long bytesWritten;
        private boolean flushDone;
        private IOException flushException;
        private int pagesFlushed;

        EvictionRecorderEvent(long cachePageId) {
            this.cachePageId = cachePageId;
        }

        @Override
        public void close() {
            this.evictionClosed = true;
        }

        @Override
        public void setFilePageId(long filePageId) {
            this.filePageId = filePageId;
        }

        @Override
        public void setSwapper(PageSwapper swapper) {
            this.swapper = swapper;
        }

        @Override
        public void setException(IOException exception) {
            this.evictionException = exception;
        }

        @Override
        public FlushEvent beginFlush(long pageRef, PageSwapper swapper, PageReferenceTranslator pageTranslator) {
            return new FlushRecorderEvent();
        }

        private class FlushRecorderEvent implements FlushEvent {
            @Override
            public void addBytesWritten(long bytes) {
                EvictionLogicTest.EvictionRecorderEvent.this.bytesWritten += bytes;
            }

            @Override
            public void close() {
                EvictionLogicTest.EvictionRecorderEvent.this.flushDone = true;
            }

            @Override
            public void setException(IOException exception) {
                EvictionLogicTest.EvictionRecorderEvent.this.flushException = exception;
            }

            @Override
            public void addPagesFlushed(int pageCount) {
                EvictionLogicTest.EvictionRecorderEvent.this.pagesFlushed += pageCount;
            }

            @Override
            public void addEvictionFlushedPages(int pageCount) {
                addPagesFlushed(pageCount);
            }

            @Override
            public void addPagesMerged(int pagesMerged) {}
        }
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void tryEvictMustReportToEvictionEvent(int pageId) throws Exception {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        PageSwapper swapper = new DummyPageSwapper("a", 313);
        int swapperId = swappers.allocate(swapper);
        doFault(swapperId, 42);
        PageMetadata.unlockExclusive(pageRef);
        EvictionRecorderEvent recorder = new EvictionRecorderEvent(pageRef);
        assertTrue(EvictionLogic.tryEvict(
                pageRef, new RecorderEvictionEventOpportunity(recorder), swappers, pageMetadata));
        assertThat(recorder.evictionClosed).isEqualTo(true);
        assertThat(recorder.filePageId).isEqualTo(42L);
        assertThat(recorder.swapper).isSameAs(swapper);
        assertThat(recorder.evictionException).isNull();
        assertThat(recorder.cachePageId).isEqualTo(pageRef);
        assertThat(recorder.bytesWritten).isEqualTo(0L);
        assertThat(recorder.flushDone).isEqualTo(false);
        assertThat(recorder.flushException).isNull();
        assertThat(recorder.pagesFlushed).isEqualTo(0);
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void tryEvictThatFlushesMustReportToEvictionAndFlushEvents(int pageId) throws Exception {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        int filePageSize = 313;
        PageSwapper swapper = new DummyPageSwapper("a", filePageSize);
        int swapperId = swappers.allocate(swapper);
        doFault(swapperId, 42);
        PageMetadata.unlockExclusiveAndTakeWriteLock(pageRef);
        PageMetadata.unlockWrite(pageRef); // page is now modified
        assertTrue(PageMetadata.isModified(pageRef));
        EvictionRecorderEvent recorder = new EvictionRecorderEvent(pageRef);
        assertTrue(EvictionLogic.tryEvict(
                pageRef, new RecorderEvictionEventOpportunity(recorder), swappers, pageMetadata));
        assertThat(recorder.evictionClosed).isEqualTo(true);
        assertThat(recorder.filePageId).isEqualTo(42L);
        assertThat(recorder.swapper).isSameAs(swapper);
        assertThat(recorder.evictionException).isNull();
        assertThat(recorder.cachePageId).isEqualTo(pageRef);
        assertThat(recorder.bytesWritten).isEqualTo(filePageSize);
        assertThat(recorder.flushDone).isEqualTo(true);
        assertThat(recorder.flushException).isNull();
        assertThat(recorder.pagesFlushed).isEqualTo(1);
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void tryEvictThatFailsMustReportExceptionsToEvictionAndFlushEvents(int pageId) throws Exception {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        IOException ioException = new IOException();
        PageSwapper swapper = new DummyPageSwapper("a", 313) {
            @Override
            public long write(long filePageId, long bufferAddress) throws IOException {
                throw ioException;
            }
        };
        int swapperId = swappers.allocate(swapper);
        doFault(swapperId, 42);
        PageMetadata.unlockExclusiveAndTakeWriteLock(pageRef);
        PageMetadata.unlockWrite(pageRef); // page is now modified
        assertTrue(PageMetadata.isModified(pageRef));
        try (EvictionRecorderEvent recorder = new EvictionRecorderEvent(pageRef)) {
            assertThrows(
                    IOException.class,
                    () -> EvictionLogic.tryEvict(
                            pageRef, new RecorderEvictionEventOpportunity(recorder), swappers, pageMetadata));

            assertThat(recorder.evictionClosed).isEqualTo(true);
            assertThat(recorder.filePageId).isEqualTo(42L);
            assertThat(recorder.swapper).isSameAs(swapper);
            assertThat(recorder.evictionException).isSameAs(ioException);
            assertThat(recorder.cachePageId).isEqualTo(pageRef);
            assertThat(recorder.bytesWritten).isEqualTo(0L);
            assertThat(recorder.flushDone).isEqualTo(true);
            assertThat(recorder.flushException).isSameAs(ioException);
            assertThat(recorder.pagesFlushed).isEqualTo(0);
        }
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void tryEvictThatSucceedsMustNotInterfereWithAdjacentPages(int pageId) throws Exception {
        init(pageId);

        PageMetadata.unlockExclusive(prevPageRef);
        PageMetadata.unlockExclusive(pageRef);
        PageMetadata.unlockExclusive(nextPageRef);
        PageSwapper swapper = new DummyPageSwapper("a", 313);
        int swapperId = swappers.allocate(swapper);
        long prevStamp = PageMetadata.tryOptimisticReadLock(prevPageRef);
        long nextStamp = PageMetadata.tryOptimisticReadLock(nextPageRef);
        doFault(swapperId, 42);
        PageMetadata.unlockExclusive(pageRef);
        assertTrue(EvictionLogic.tryEvict(pageRef, EvictionRunEvent.NULL, swappers, pageMetadata));
        assertTrue(PageMetadata.validateReadLock(prevPageRef, prevStamp));
        assertTrue(PageMetadata.validateReadLock(nextPageRef, nextStamp));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void tryEvictThatFlushesAndSucceedsMustNotInterfereWithAdjacentPages(int pageId) throws Exception {
        init(pageId);

        PageMetadata.unlockExclusive(prevPageRef);
        PageMetadata.unlockExclusive(pageRef);
        PageMetadata.unlockExclusive(nextPageRef);
        PageSwapper swapper = new DummyPageSwapper("a", 313);
        int swapperId = swappers.allocate(swapper);
        long prevStamp = PageMetadata.tryOptimisticReadLock(prevPageRef);
        long nextStamp = PageMetadata.tryOptimisticReadLock(nextPageRef);
        doFault(swapperId, 42);
        PageMetadata.unlockExclusiveAndTakeWriteLock(pageRef);
        PageMetadata.unlockWrite(pageRef); // page is now modified
        assertTrue(PageMetadata.isModified(pageRef));
        assertTrue(EvictionLogic.tryEvict(pageRef, EvictionRunEvent.NULL, swappers, pageMetadata));
        assertTrue(PageMetadata.validateReadLock(prevPageRef, prevStamp));
        assertTrue(PageMetadata.validateReadLock(nextPageRef, nextStamp));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void tryEvictThatFailsMustNotInterfereWithAdjacentPages(int pageId) throws Exception {
        init(pageId);

        PageMetadata.unlockExclusive(prevPageRef);
        PageMetadata.unlockExclusive(pageRef);
        PageMetadata.unlockExclusive(nextPageRef);
        PageSwapper swapper = new DummyPageSwapper("a", 313) {
            @Override
            public long write(long filePageId, long bufferAddress) throws IOException {
                throw new IOException();
            }
        };
        int swapperId = swappers.allocate(swapper);
        long prevStamp = PageMetadata.tryOptimisticReadLock(prevPageRef);
        long nextStamp = PageMetadata.tryOptimisticReadLock(nextPageRef);
        doFault(swapperId, 42);
        PageMetadata.unlockExclusiveAndTakeWriteLock(pageRef);
        PageMetadata.unlockWrite(pageRef); // page is now modified
        assertTrue(PageMetadata.isModified(pageRef));
        assertThatThrownBy(() -> EvictionLogic.tryEvict(pageRef, EvictionRunEvent.NULL, swappers, pageMetadata))
                .isInstanceOf(IOException.class);
        assertTrue(PageMetadata.validateReadLock(prevPageRef, prevStamp));
        assertTrue(PageMetadata.validateReadLock(nextPageRef, nextStamp));
    }

    private void doFault(int swapperId, long filePageId) throws IOException {
        assertTrue(PageMetadata.tryExclusiveLock(pageRef));
        MuninnPagedFile.validatePageRefAndSetFilePageId(pageRef, DUMMY_SWAPPER, swapperId, filePageId);
        pageMetadata.ensurePageAllocated(pageRef);
        MuninnPageCursor.fault(pageRef, DUMMY_SWAPPER, swapperId, filePageId, PinPageFaultEvent.NULL);
    }

    private static class RecorderEvictionEventOpportunity implements EvictionEventOpportunity {
        private final EvictionRecorderEvent recorder;

        public RecorderEvictionEventOpportunity(EvictionRecorderEvent recorder) {
            this.recorder = recorder;
        }

        @Override
        public EvictionEvent beginEviction(long cachePageId) {
            return recorder;
        }

        @Override
        public AsyncEvictionEvent beginAsyncEviction(long cachePageId) {
            return AsyncEvictionEvent.NULL;
        }
    }
}
