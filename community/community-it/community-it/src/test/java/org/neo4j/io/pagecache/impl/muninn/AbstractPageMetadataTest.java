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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntFunction;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.impl.muninn.swapper.PageSwapper;
import org.neo4j.io.pagecache.tracing.DummyPageSwapper;
import org.neo4j.io.pagecache.tracing.PinPageFaultEvent;
import org.neo4j.test.scheduler.DaemonThreadFactory;
import org.neo4j.util.concurrent.Futures;

public class AbstractPageMetadataTest {
    protected static final Duration TIMEOUT = Duration.ofMinutes(1);

    private static final int[] pageIds = new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    private static final DummyPageSwapper DUMMY_SWAPPER = new DummyPageSwapper("", UnsafeUtil.pageSize());

    private static Stream<Arguments> argumentsProvider() {
        IntFunction<Arguments> toArguments = Arguments::of;
        return Arrays.stream(pageIds).mapToObj(toArguments);
    }

    protected ExecutorService executor;

    @BeforeEach
    void setUpAbstract() {
        executor = Executors.newCachedThreadPool(new DaemonThreadFactory());
    }

    @AfterEach
    void tearDownAbstract() {
        if (pageMetadata != null) {
            pageMetadata.close();
            pageMetadata = null;
        }
        executor.shutdown();
        executor = null;
    }

    private int prevPageId;
    private int nextPageId;
    protected long pageRef;
    private long prevPageRef;
    private long nextPageRef;
    private int pageSize;
    private PageMetadata pageMetadata;
    protected boolean singleWriter;

    protected void init(int pageId) {
        prevPageId = pageId == 0 ? pageIds.length - 1 : (pageId - 1) % pageIds.length;
        nextPageId = (pageId + 1) % pageIds.length;
        pageSize = UnsafeUtil.pageSize();

        pageMetadata = new PageMetadata(pageIds.length, pageSize);
        pageRef = pageMetadata.deref(pageId);
        prevPageRef = pageMetadata.deref(prevPageId);
        nextPageRef = pageMetadata.deref(nextPageId);
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void mustExposePageCount(int pageId) {
        init(pageId);

        int pageCount;

        pageCount = 3;
        try (PageMetadata metadata = new PageMetadata(pageCount, pageSize)) {
            assertThat(metadata.getPageCount()).isEqualTo(pageCount);
        }

        pageCount = 42;
        try (PageMetadata metadata = new PageMetadata(pageCount, pageSize)) {
            assertThat(metadata.getPageCount()).isEqualTo(pageCount);
        }
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void mustBeAbleToReversePageRedToPageId(int pageId) {
        init(pageId);

        assertThat(pageMetadata.toId(pageRef)).isEqualTo(pageId);
    }

    // xxx ---[ Sequence lock tests ]---

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void pagesAreInitiallyExclusivelyLocked(int pageId) {
        init(pageId);

        assertTrue(PageMetadata.isExclusivelyLocked(pageRef));
        PageMetadata.unlockExclusive(pageRef);
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void uncontendedOptimisticLockMustValidate(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        long stamp = PageMetadata.tryOptimisticReadLock(pageRef);
        assertTrue(PageMetadata.validateReadLock(pageRef, stamp));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void mustNotValidateRandomStamp(int pageId) {
        init(pageId);

        assertFalse(PageMetadata.validateReadLock(pageRef, 4242));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void writeLockMustInvalidateOptimisticReadLock(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        long r = PageMetadata.tryOptimisticReadLock(pageRef);
        PageMetadata.tryWriteLock(pageRef, singleWriter);
        PageMetadata.unlockWrite(pageRef);
        assertFalse(PageMetadata.validateReadLock(pageRef, r));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void takingWriteLockMustInvalidateOptimisticReadLock(int pageId) {
        init(pageId);

        long r = PageMetadata.tryOptimisticReadLock(pageRef);
        PageMetadata.tryWriteLock(pageRef, singleWriter);
        assertFalse(PageMetadata.validateReadLock(pageRef, r));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void optimisticReadLockMustNotValidateUnderWriteLock(int pageId) {
        init(pageId);

        PageMetadata.tryWriteLock(pageRef, singleWriter);
        long r = PageMetadata.tryOptimisticReadLock(pageRef);
        assertFalse(PageMetadata.validateReadLock(pageRef, r));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void writeLockReleaseMustInvalidateOptimisticReadLock(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        PageMetadata.tryWriteLock(pageRef, singleWriter);
        long r = PageMetadata.tryOptimisticReadLock(pageRef);
        PageMetadata.unlockWrite(pageRef);
        assertFalse(PageMetadata.validateReadLock(pageRef, r));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void uncontendedWriteLockMustBeAvailable(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        assertTrue(PageMetadata.tryWriteLock(pageRef, singleWriter));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void uncontendedOptimisticReadLockMustValidateAfterWriteLockRelease(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        PageMetadata.tryWriteLock(pageRef, singleWriter);
        PageMetadata.unlockWrite(pageRef);
        long r = PageMetadata.tryOptimisticReadLock(pageRef);
        assertTrue(PageMetadata.validateReadLock(pageRef, r));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void unmatchedUnlockWriteLockMustThrow(int pageId) {
        init(pageId);

        assertThrows(IllegalMonitorStateException.class, () -> PageMetadata.unlockWrite(pageRef));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void exclusiveLockMustInvalidateOptimisticLock(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        long r = PageMetadata.tryOptimisticReadLock(pageRef);
        PageMetadata.tryExclusiveLock(pageRef);
        PageMetadata.unlockExclusive(pageRef);
        assertFalse(PageMetadata.validateReadLock(pageRef, r));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void takingExclusiveLockMustInvalidateOptimisticLock(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        long r = PageMetadata.tryOptimisticReadLock(pageRef);
        PageMetadata.tryExclusiveLock(pageRef);
        assertFalse(PageMetadata.validateReadLock(pageRef, r));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void optimisticReadLockMustNotValidateUnderExclusiveLock(int pageId) {
        init(pageId);

        // exclusive lock implied by constructor
        long r = PageMetadata.tryOptimisticReadLock(pageRef);
        assertFalse(PageMetadata.validateReadLock(pageRef, r));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void exclusiveLockReleaseMustInvalidateOptimisticReadLock(int pageId) {
        init(pageId);

        // exclusive lock implied by constructor
        long r = PageMetadata.tryOptimisticReadLock(pageRef);
        PageMetadata.unlockExclusive(pageRef);
        assertFalse(PageMetadata.validateReadLock(pageRef, r));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void uncontendedOptimisticReadLockMustValidateAfterExclusiveLockRelease(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        PageMetadata.tryExclusiveLock(pageRef);
        PageMetadata.unlockExclusive(pageRef);
        long r = PageMetadata.tryOptimisticReadLock(pageRef);
        assertTrue(PageMetadata.validateReadLock(pageRef, r));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void canTakeUncontendedExclusiveLocks(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        assertTrue(PageMetadata.tryExclusiveLock(pageRef));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void writeLocksMustFailExclusiveLocks(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        PageMetadata.tryWriteLock(pageRef, singleWriter);
        assertFalse(PageMetadata.tryExclusiveLock(pageRef));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void exclusiveLockMustBeAvailableAfterWriteLock(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        PageMetadata.tryWriteLock(pageRef, singleWriter);
        PageMetadata.unlockWrite(pageRef);
        assertTrue(PageMetadata.tryExclusiveLock(pageRef));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void cannotTakeExclusiveLockIfAlreadyTaken(int pageId) {
        init(pageId);

        // existing exclusive lock implied by constructor
        assertFalse(PageMetadata.tryExclusiveLock(pageRef));
        PageMetadata.unlockExclusive(pageRef);
        assertTrue(PageMetadata.tryExclusiveLock(pageRef));
        assertFalse(PageMetadata.tryExclusiveLock(pageRef));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void exclusiveLockMustBeAvailableAfterExclusiveLock(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        assertTrue(PageMetadata.tryExclusiveLock(pageRef));
        PageMetadata.unlockExclusive(pageRef);
        assertTrue(PageMetadata.tryExclusiveLock(pageRef));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void exclusiveLockMustFailWriteLocks(int pageId) {
        init(pageId);

        assertTimeoutPreemptively(TIMEOUT, () -> {
            // exclusive lock implied by constructor
            assertFalse(PageMetadata.tryWriteLock(pageRef, singleWriter));
        });
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void unmatchedUnlockExclusiveLockMustThrow(int pageId) {
        init(pageId);

        assertThrows(IllegalMonitorStateException.class, () -> {
            PageMetadata.unlockExclusive(pageRef);
            PageMetadata.unlockExclusive(pageRef);
        });
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void unmatchedUnlockWriteAfterTakingExclusiveLockMustThrow(int pageId) {
        init(pageId);

        assertThrows(IllegalMonitorStateException.class, () -> {
            PageMetadata.unlockExclusive(pageRef);
            PageMetadata.tryExclusiveLock(pageRef);
            PageMetadata.unlockWrite(pageRef);
        });
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void writeLockMustBeAvailableAfterExclusiveLock(int pageId) {
        init(pageId);

        assertTimeoutPreemptively(TIMEOUT, () -> {
            PageMetadata.unlockExclusive(pageRef);
            PageMetadata.tryExclusiveLock(pageRef);
            PageMetadata.unlockExclusive(pageRef);
            assertTrue(PageMetadata.tryWriteLock(pageRef, singleWriter));
            PageMetadata.unlockWrite(pageRef);
        });
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void unlockExclusiveMustReturnStampForOptimisticReadLock(int pageId) {
        init(pageId);

        // exclusive lock implied by constructor
        long r = PageMetadata.unlockExclusive(pageRef);
        assertTrue(PageMetadata.validateReadLock(pageRef, r));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void unlockExclusiveAndTakeWriteLockMustInvalidateOptimisticReadLocks(int pageId) {
        init(pageId);

        // exclusive lock implied by constructor
        PageMetadata.unlockExclusiveAndTakeWriteLock(pageRef);
        long r = PageMetadata.tryOptimisticReadLock(pageRef);
        assertFalse(PageMetadata.validateReadLock(pageRef, r));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void unlockExclusiveAndTakeWriteLockMustPreventExclusiveLocks(int pageId) {
        init(pageId);

        // exclusive lock implied by constructor
        PageMetadata.unlockExclusiveAndTakeWriteLock(pageRef);
        assertFalse(PageMetadata.tryExclusiveLock(pageRef));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void unlockExclusiveAndTakeWriteLockMustBeAtomic(int pageId) {
        init(pageId);

        assertTimeoutPreemptively(TIMEOUT, () -> {
            // exclusive lock implied by constructor
            int threads = Runtime.getRuntime().availableProcessors() - 1;
            CountDownLatch start = new CountDownLatch(threads);
            AtomicBoolean stop = new AtomicBoolean();
            PageMetadata.tryExclusiveLock(pageRef);
            Runnable runnable = () -> {
                while (!stop.get()) {
                    if (PageMetadata.tryExclusiveLock(pageRef)) {
                        PageMetadata.unlockExclusive(pageRef);
                        throw new RuntimeException("I should not have gotten that lock");
                    }
                    start.countDown();
                }
            };

            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                futures.add(executor.submit(runnable));
            }

            start.await();
            PageMetadata.unlockExclusiveAndTakeWriteLock(pageRef);
            stop.set(true);
            Futures.getAll(futures);
        });
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void stampFromUnlockExclusiveMustNotBeValidIfThereAreWriteLocks(int pageId) {
        init(pageId);

        // exclusive lock implied by constructor
        long r = PageMetadata.unlockExclusive(pageRef);
        assertTrue(PageMetadata.tryWriteLock(pageRef, singleWriter));
        assertFalse(PageMetadata.validateReadLock(pageRef, r));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void uncontendedFlushLockMustBeAvailable(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        assertTrue(PageMetadata.tryFlushLock(pageRef) != 0);
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void flushLockMustNotInvalidateOptimisticReadLock(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        long r = PageMetadata.tryOptimisticReadLock(pageRef);
        long s = PageMetadata.tryFlushLock(pageRef);
        PageMetadata.unlockFlush(pageRef, s, true);
        assertTrue(PageMetadata.validateReadLock(pageRef, r));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void flushLockMustNotFailWriteLock(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        PageMetadata.tryFlushLock(pageRef);
        assertTrue(PageMetadata.tryWriteLock(pageRef, singleWriter));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void flushLockMustFailExclusiveLock(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        PageMetadata.tryFlushLock(pageRef);
        assertFalse(PageMetadata.tryExclusiveLock(pageRef));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void cannotTakeFlushLockIfAlreadyTaken(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        assertTrue(PageMetadata.tryFlushLock(pageRef) != 0);
        assertFalse(PageMetadata.tryFlushLock(pageRef) != 0);
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void writeLockMustNotFailFlushLock(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        PageMetadata.tryWriteLock(pageRef, singleWriter);
        assertTrue(PageMetadata.tryFlushLock(pageRef) != 0);
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void exclusiveLockMustFailFlushLock(int pageId) {
        init(pageId);

        // exclusively locked from constructor
        assertFalse(PageMetadata.tryFlushLock(pageRef) != 0);
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void unlockExclusiveAndTakeWriteLockMustNotFailFlushLock(int pageId) {
        init(pageId);

        // exclusively locked from constructor
        PageMetadata.unlockExclusiveAndTakeWriteLock(pageRef);
        assertTrue(PageMetadata.tryFlushLock(pageRef) != 0);
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void flushUnlockMustNotInvalidateOptimisticReadLock(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        long r = PageMetadata.tryOptimisticReadLock(pageRef);
        assertTrue(PageMetadata.tryFlushLock(pageRef) != 0);
        assertTrue(PageMetadata.validateReadLock(pageRef, r));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void optimisticReadLockMustValidateUnderFlushLock(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        PageMetadata.tryFlushLock(pageRef);
        long r = PageMetadata.tryOptimisticReadLock(pageRef);
        assertTrue(PageMetadata.validateReadLock(pageRef, r));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void flushLockReleaseMustNotInvalidateOptimisticReadLock(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        long s = PageMetadata.tryFlushLock(pageRef);
        long r = PageMetadata.tryOptimisticReadLock(pageRef);
        PageMetadata.unlockFlush(pageRef, s, true);
        assertTrue(PageMetadata.validateReadLock(pageRef, r));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void unmatchedUnlockFlushMustThrow(int pageId) {
        init(pageId);

        assertThrows(
                IllegalMonitorStateException.class,
                () -> PageMetadata.unlockFlush(pageRef, PageMetadata.tryOptimisticReadLock(pageRef), true));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void uncontendedOptimisticReadLockMustBeAvailableAfterFlushLock(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        long s = PageMetadata.tryFlushLock(pageRef);
        PageMetadata.unlockFlush(pageRef, s, true);
        long r = PageMetadata.tryOptimisticReadLock(pageRef);
        assertTrue(PageMetadata.validateReadLock(pageRef, r));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void uncontendedWriteLockMustBeAvailableAfterFlushLock(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        long s = PageMetadata.tryFlushLock(pageRef);
        PageMetadata.unlockFlush(pageRef, s, true);
        assertTrue(PageMetadata.tryWriteLock(pageRef, singleWriter));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void uncontendedExclusiveLockMustBeAvailableAfterFlushLock(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        long s = PageMetadata.tryFlushLock(pageRef);
        PageMetadata.unlockFlush(pageRef, s, true);
        assertTrue(PageMetadata.tryExclusiveLock(pageRef));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void uncontendedFlushLockMustBeAvailableAfterWriteLock(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        PageMetadata.tryWriteLock(pageRef, singleWriter);
        PageMetadata.unlockWrite(pageRef);
        assertTrue(PageMetadata.tryFlushLock(pageRef) != 0);
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void uncontendedFlushLockMustBeAvailableAfterExclusiveLock(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        PageMetadata.tryExclusiveLock(pageRef);
        PageMetadata.unlockExclusive(pageRef);
        assertTrue(PageMetadata.tryFlushLock(pageRef) != 0);
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void uncontendedFlushLockMustBeAvailableAfterFlushLock(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        long s = PageMetadata.tryFlushLock(pageRef);
        PageMetadata.unlockFlush(pageRef, s, true);
        assertTrue(PageMetadata.tryFlushLock(pageRef) != 0);
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void stampFromUnlockExclusiveMustBeValidUnderFlushLock(int pageId) {
        init(pageId);

        // exclusively locked from constructor
        long r = PageMetadata.unlockExclusive(pageRef);
        PageMetadata.tryFlushLock(pageRef);
        assertTrue(PageMetadata.validateReadLock(pageRef, r));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void optimisticReadLockMustNotGetInterferenceFromAdjacentWriteLocks(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(prevPageRef);
        PageMetadata.unlockExclusive(pageRef);
        PageMetadata.unlockExclusive(nextPageRef);
        assertTrue(PageMetadata.tryWriteLock(prevPageRef, singleWriter));
        assertTrue(PageMetadata.tryWriteLock(nextPageRef, singleWriter));
        long r = PageMetadata.tryOptimisticReadLock(pageRef);
        assertTrue(PageMetadata.validateReadLock(pageRef, r));
        PageMetadata.unlockWrite(prevPageRef);
        PageMetadata.unlockWrite(nextPageRef);
        assertTrue(PageMetadata.validateReadLock(pageRef, r));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void optimisticReadLockMustNotGetInterferenceFromAdjacentExclusiveLocks(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(prevPageRef);
        PageMetadata.unlockExclusive(pageRef);
        PageMetadata.unlockExclusive(nextPageRef);
        assertTrue(PageMetadata.tryExclusiveLock(prevPageRef));
        assertTrue(PageMetadata.tryExclusiveLock(nextPageRef));
        long r = PageMetadata.tryOptimisticReadLock(pageRef);
        assertTrue(PageMetadata.validateReadLock(pageRef, r));
        PageMetadata.unlockExclusive(prevPageRef);
        PageMetadata.unlockExclusive(nextPageRef);
        assertTrue(PageMetadata.validateReadLock(pageRef, r));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void optimisticReadLockMustNotGetInterferenceFromAdjacentExclusiveAndWriteLocks(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(prevPageRef);
        PageMetadata.unlockExclusive(pageRef);
        PageMetadata.unlockExclusive(nextPageRef);
        assertTrue(PageMetadata.tryExclusiveLock(prevPageRef));
        assertTrue(PageMetadata.tryExclusiveLock(nextPageRef));
        long r = PageMetadata.tryOptimisticReadLock(pageRef);
        assertTrue(PageMetadata.validateReadLock(pageRef, r));
        PageMetadata.unlockExclusiveAndTakeWriteLock(prevPageRef);
        PageMetadata.unlockExclusiveAndTakeWriteLock(nextPageRef);
        assertTrue(PageMetadata.validateReadLock(pageRef, r));
        PageMetadata.unlockWrite(prevPageRef);
        PageMetadata.unlockWrite(nextPageRef);
        assertTrue(PageMetadata.validateReadLock(pageRef, r));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void writeLockMustNotGetInterferenceFromAdjacentExclusiveLocks(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(prevPageRef);
        PageMetadata.unlockExclusive(pageRef);
        PageMetadata.unlockExclusive(nextPageRef);
        assertTrue(PageMetadata.tryExclusiveLock(prevPageRef));
        assertTrue(PageMetadata.tryExclusiveLock(nextPageRef));
        assertTrue(PageMetadata.tryWriteLock(pageRef, singleWriter));
        PageMetadata.unlockWrite(pageRef);
        PageMetadata.unlockExclusive(prevPageRef);
        PageMetadata.unlockExclusive(nextPageRef);
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void flushLockMustNotGetInterferenceFromAdjacentExclusiveLocks(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(prevPageRef);
        PageMetadata.unlockExclusive(pageRef);
        PageMetadata.unlockExclusive(nextPageRef);
        long s;
        assertTrue(PageMetadata.tryExclusiveLock(prevPageRef));
        assertTrue(PageMetadata.tryExclusiveLock(nextPageRef));
        assertTrue((s = PageMetadata.tryFlushLock(pageRef)) != 0);
        PageMetadata.unlockFlush(pageRef, s, true);
        PageMetadata.unlockExclusive(prevPageRef);
        PageMetadata.unlockExclusive(nextPageRef);
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void flushLockMustNotGetInterferenceFromAdjacentFlushLocks(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(prevPageRef);
        PageMetadata.unlockExclusive(pageRef);
        PageMetadata.unlockExclusive(nextPageRef);
        long ps;
        long ns;
        long s;
        assertTrue((ps = PageMetadata.tryFlushLock(prevPageRef)) != 0);
        assertTrue((ns = PageMetadata.tryFlushLock(nextPageRef)) != 0);
        assertTrue((s = PageMetadata.tryFlushLock(pageRef)) != 0);
        PageMetadata.unlockFlush(pageRef, s, true);
        PageMetadata.unlockFlush(prevPageRef, ps, true);
        PageMetadata.unlockFlush(nextPageRef, ns, true);
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void exclusiveLockMustNotGetInterferenceFromAdjacentExclusiveLocks(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        PageMetadata.unlockExclusive(prevPageRef);
        PageMetadata.unlockExclusive(nextPageRef);
        assertTrue(PageMetadata.tryExclusiveLock(prevPageRef));
        assertTrue(PageMetadata.tryExclusiveLock(nextPageRef));
        assertTrue(PageMetadata.tryExclusiveLock(pageRef));
        PageMetadata.unlockExclusive(pageRef);
        PageMetadata.unlockExclusive(prevPageRef);
        PageMetadata.unlockExclusive(nextPageRef);
        assertTrue(PageMetadata.tryExclusiveLock(prevPageRef));
        assertTrue(PageMetadata.tryExclusiveLock(nextPageRef));
        assertTrue(PageMetadata.tryExclusiveLock(pageRef));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void exclusiveLockMustNotGetInterferenceFromAdjacentWriteLocks(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        PageMetadata.unlockExclusive(prevPageRef);
        PageMetadata.unlockExclusive(nextPageRef);
        assertTrue(PageMetadata.tryWriteLock(prevPageRef, singleWriter));
        assertTrue(PageMetadata.tryWriteLock(nextPageRef, singleWriter));
        assertTrue(PageMetadata.tryExclusiveLock(pageRef));
        PageMetadata.unlockExclusive(pageRef);
        PageMetadata.unlockWrite(prevPageRef);
        PageMetadata.unlockWrite(nextPageRef);
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void exclusiveLockMustNotGetInterferenceFromAdjacentExclusiveAndWriteLocks(int pageId) {
        init(pageId);

        // exclusive locks on prevPageRef, nextPageRef and pageRef are implied from constructor
        PageMetadata.unlockExclusive(pageRef);
        PageMetadata.unlockExclusiveAndTakeWriteLock(prevPageRef);
        PageMetadata.unlockExclusiveAndTakeWriteLock(nextPageRef);
        assertTrue(PageMetadata.tryExclusiveLock(pageRef));
        PageMetadata.unlockExclusive(pageRef);
        PageMetadata.unlockWrite(prevPageRef);
        PageMetadata.unlockWrite(nextPageRef);

        assertTrue(PageMetadata.tryExclusiveLock(pageRef));
        assertTrue(PageMetadata.tryExclusiveLock(prevPageRef));
        assertTrue(PageMetadata.tryExclusiveLock(nextPageRef));
        PageMetadata.unlockExclusiveAndTakeWriteLock(prevPageRef);
        PageMetadata.unlockExclusiveAndTakeWriteLock(nextPageRef);
        PageMetadata.unlockWrite(prevPageRef);
        PageMetadata.unlockWrite(nextPageRef);
        PageMetadata.unlockExclusive(pageRef);
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void exclusiveLockMustNotGetInterferenceFromAdjacentFlushLocks(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(prevPageRef);
        PageMetadata.unlockExclusive(pageRef);
        PageMetadata.unlockExclusive(nextPageRef);
        long ps;
        long ns;
        assertTrue((ps = PageMetadata.tryFlushLock(prevPageRef)) != 0);
        assertTrue((ns = PageMetadata.tryFlushLock(nextPageRef)) != 0);
        assertTrue(PageMetadata.tryExclusiveLock(pageRef));
        PageMetadata.unlockExclusive(pageRef);
        PageMetadata.unlockFlush(prevPageRef, ps, true);
        PageMetadata.unlockFlush(nextPageRef, ns, true);
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void takingWriteLockMustRaiseModifiedFlag(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        assertFalse(PageMetadata.isModified(pageRef));
        assertTrue(PageMetadata.tryWriteLock(pageRef, singleWriter));
        assertTrue(PageMetadata.isModified(pageRef));
        PageMetadata.unlockWrite(pageRef);
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void turningExclusiveLockIntoWriteLockMustRaiseModifiedFlag(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        assertFalse(PageMetadata.isModified(pageRef));
        assertTrue(PageMetadata.tryExclusiveLock(pageRef));
        assertFalse(PageMetadata.isModified(pageRef));
        PageMetadata.unlockExclusiveAndTakeWriteLock(pageRef);
        assertTrue(PageMetadata.isModified(pageRef));
        PageMetadata.unlockWrite(pageRef);
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void releasingFlushLockMustLowerModifiedFlagIfSuccessful(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        assertTrue(PageMetadata.tryWriteLock(pageRef, singleWriter));
        PageMetadata.unlockWrite(pageRef);
        assertTrue(PageMetadata.isModified(pageRef));
        long s = PageMetadata.tryFlushLock(pageRef);
        PageMetadata.unlockFlush(pageRef, s, true);
        assertFalse(PageMetadata.isModified(pageRef));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void loweredModifiedFlagMustRemainLoweredAfterReleasingFlushLock(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        assertTrue(PageMetadata.tryWriteLock(pageRef, singleWriter));
        PageMetadata.unlockWrite(pageRef);
        assertTrue(PageMetadata.isModified(pageRef));
        long s = PageMetadata.tryFlushLock(pageRef);
        PageMetadata.unlockFlush(pageRef, s, true);
        assertFalse(PageMetadata.isModified(pageRef));

        s = PageMetadata.tryFlushLock(pageRef);
        PageMetadata.unlockFlush(pageRef, s, true);
        assertFalse(PageMetadata.isModified(pageRef));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void releasingFlushLockMustNotLowerModifiedFlagIfUnsuccessful(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        assertTrue(PageMetadata.tryWriteLock(pageRef, singleWriter));
        PageMetadata.unlockWrite(pageRef);
        assertTrue(PageMetadata.isModified(pageRef));
        long s = PageMetadata.tryFlushLock(pageRef);
        PageMetadata.unlockFlush(pageRef, s, false);
        assertTrue(PageMetadata.isModified(pageRef));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void releasingFlushLockMustNotLowerModifiedFlagIfWriteLockWasWithinFlushFlushLock(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        long s = PageMetadata.tryFlushLock(pageRef);
        assertTrue(PageMetadata.tryWriteLock(pageRef, singleWriter));
        PageMetadata.unlockWrite(pageRef);
        PageMetadata.unlockFlush(pageRef, s, true);
        assertTrue(PageMetadata.isModified(pageRef));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void releasingFlushLockMustNotLowerModifiedFlagIfWriteLockOverlappedTakingFlushLock(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        assertTrue(PageMetadata.tryWriteLock(pageRef, singleWriter));
        long s = PageMetadata.tryFlushLock(pageRef);
        PageMetadata.unlockWrite(pageRef);
        PageMetadata.unlockFlush(pageRef, s, true);
        assertTrue(PageMetadata.isModified(pageRef));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void releasingFlushLockMustNotLowerModifiedFlagIfWriteLockOverlappedReleasingFlushLock(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        long s = PageMetadata.tryFlushLock(pageRef);
        assertTrue(PageMetadata.tryWriteLock(pageRef, singleWriter));
        PageMetadata.unlockFlush(pageRef, s, true);
        assertTrue(PageMetadata.isModified(pageRef));
        PageMetadata.unlockWrite(pageRef);
        assertTrue(PageMetadata.isModified(pageRef));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void releasingFlushLockMustNotLowerModifiedFlagIfWriteLockOverlappedFlushLock(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        assertTrue(PageMetadata.tryWriteLock(pageRef, singleWriter));
        long s = PageMetadata.tryFlushLock(pageRef);
        PageMetadata.unlockFlush(pageRef, s, true);
        assertTrue(PageMetadata.isModified(pageRef));
        PageMetadata.unlockWrite(pageRef);
        assertTrue(PageMetadata.isModified(pageRef));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void releasingFlushLockMustNotInterfereWithAdjacentModifiedFlags(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(prevPageRef);
        PageMetadata.unlockExclusive(pageRef);
        PageMetadata.unlockExclusive(nextPageRef);
        assertTrue(PageMetadata.tryWriteLock(prevPageRef, singleWriter));
        assertTrue(PageMetadata.tryWriteLock(pageRef, singleWriter));
        assertTrue(PageMetadata.tryWriteLock(nextPageRef, singleWriter));
        PageMetadata.unlockWrite(prevPageRef);
        PageMetadata.unlockWrite(pageRef);
        PageMetadata.unlockWrite(nextPageRef);
        assertTrue(PageMetadata.isModified(prevPageRef));
        assertTrue(PageMetadata.isModified(pageRef));
        assertTrue(PageMetadata.isModified(nextPageRef));
        long s = PageMetadata.tryFlushLock(pageRef);
        PageMetadata.unlockFlush(pageRef, s, true);
        assertTrue(PageMetadata.isModified(prevPageRef));
        assertFalse(PageMetadata.isModified(pageRef));
        assertTrue(PageMetadata.isModified(nextPageRef));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void writeLockMustNotInterfereWithAdjacentModifiedFlags(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(prevPageRef);
        PageMetadata.unlockExclusive(pageRef);
        PageMetadata.unlockExclusive(nextPageRef);
        assertTrue(PageMetadata.tryWriteLock(pageRef, singleWriter));
        PageMetadata.unlockWrite(pageRef);
        assertFalse(PageMetadata.isModified(prevPageRef));
        assertTrue(PageMetadata.isModified(pageRef));
        assertFalse(PageMetadata.isModified(nextPageRef));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void disallowUnlockedPageToExplicitlyLowerModifiedFlag(int pageId) {
        init(pageId);

        assertThrows(IllegalStateException.class, () -> {
            PageMetadata.unlockExclusive(pageRef);
            PageMetadata.explicitlyMarkPageUnmodifiedUnderExclusiveLock(pageRef);
        });
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void disallowReadLockedPageToExplicitlyLowerModifiedFlag(int pageId) {
        init(pageId);

        assertThrows(IllegalStateException.class, () -> {
            PageMetadata.unlockExclusive(pageRef);
            PageMetadata.tryOptimisticReadLock(pageRef);
            PageMetadata.explicitlyMarkPageUnmodifiedUnderExclusiveLock(pageRef);
        });
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void disallowFlushLockedPageToExplicitlyLowerModifiedFlag(int pageId) {
        init(pageId);

        assertThrows(IllegalStateException.class, () -> {
            PageMetadata.unlockExclusive(pageRef);
            assertThat(PageMetadata.tryFlushLock(pageRef)).isNotEqualTo(0L);
            PageMetadata.explicitlyMarkPageUnmodifiedUnderExclusiveLock(pageRef);
        });
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void disallowWriteLockedPageToExplicitlyLowerModifiedFlag(int pageId) {
        init(pageId);

        assertThrows(IllegalStateException.class, () -> {
            PageMetadata.unlockExclusive(pageRef);
            assertTrue(PageMetadata.tryWriteLock(pageRef, singleWriter));
            PageMetadata.explicitlyMarkPageUnmodifiedUnderExclusiveLock(pageRef);
        });
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void allowExclusiveLockedPageToExplicitlyLowerModifiedFlag(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        assertFalse(PageMetadata.isModified(pageRef));
        assertTrue(PageMetadata.tryWriteLock(pageRef, singleWriter));
        PageMetadata.unlockWrite(pageRef);
        assertTrue(PageMetadata.isModified(pageRef));
        assertTrue(PageMetadata.tryExclusiveLock(pageRef));
        assertTrue(PageMetadata.isModified(pageRef));
        PageMetadata.explicitlyMarkPageUnmodifiedUnderExclusiveLock(pageRef);
        assertFalse(PageMetadata.isModified(pageRef));
        PageMetadata.unlockExclusive(pageRef);
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void unlockWriteAndTryTakeFlushLockMustTakeFlushLock(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        assertTrue(PageMetadata.tryWriteLock(pageRef, singleWriter));
        long flushStamp = PageMetadata.unlockWriteAndTryTakeFlushLock(pageRef);
        assertThat(flushStamp).isNotEqualTo(0L);
        assertThat(PageMetadata.tryFlushLock(pageRef)).isEqualTo(0L);
        PageMetadata.unlockFlush(pageRef, flushStamp, true);
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void unlockWriteAndTryTakeFlushLockMustThrowIfNotWriteLocked(int pageId) {
        init(pageId);

        assertThrows(IllegalMonitorStateException.class, () -> {
            PageMetadata.unlockExclusive(pageRef);
            PageMetadata.unlockWriteAndTryTakeFlushLock(pageRef);
        });
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void unlockWriteAndTryTakeFlushLockMustThrowIfNotWriteLockedButExclusiveLocked(int pageId) {
        init(pageId);

        assertThrows(
                IllegalMonitorStateException.class,
                () ->
                        // exclusive lock implied by constructor
                        PageMetadata.unlockWriteAndTryTakeFlushLock(pageRef));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void unlockWriteAndTryTakeFlushLockMustFailIfFlushLockIsAlreadyTaken(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusiveAndTakeWriteLock(pageRef);
        long stamp = PageMetadata.tryFlushLock(pageRef);
        assertThat(stamp).isNotEqualTo(0L);
        long secondStamp = PageMetadata.unlockWriteAndTryTakeFlushLock(pageRef);
        assertThat(secondStamp).isEqualTo(0L);
        PageMetadata.unlockFlush(pageRef, stamp, true);
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void unlockWriteAndTryTakeFlushLockMustReleaseWriteLockEvenIfFlushLockFails(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusiveAndTakeWriteLock(pageRef);
        long flushStamp = PageMetadata.tryFlushLock(pageRef);
        assertThat(flushStamp).isNotEqualTo(0L);
        assertThat(PageMetadata.unlockWriteAndTryTakeFlushLock(pageRef)).isEqualTo(0L);
        long readStamp = PageMetadata.tryOptimisticReadLock(pageRef);
        assertTrue(PageMetadata.validateReadLock(pageRef, readStamp));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void unlockWriteAndTryTakeFlushLockMustReleaseWriteLockWhenFlushLockSucceeds(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusiveAndTakeWriteLock(pageRef);
        assertThat(PageMetadata.unlockWriteAndTryTakeFlushLock(pageRef)).isNotEqualTo(0L);
        long readStamp = PageMetadata.tryOptimisticReadLock(pageRef);
        assertTrue(PageMetadata.validateReadLock(pageRef, readStamp));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void unlockWriteAndTrueTakeFlushLockMustRaiseModifiedFlag(int pageId) {
        init(pageId);

        assertFalse(PageMetadata.isModified(pageRef));
        PageMetadata.unlockExclusiveAndTakeWriteLock(pageRef);
        assertTrue(PageMetadata.isModified(pageRef));
        assertThat(PageMetadata.unlockWriteAndTryTakeFlushLock(pageRef)).isNotEqualTo(0L);
        assertTrue(PageMetadata.isModified(pageRef));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void unlockWriteAndTryTakeFlushLockAndThenUnlockFlushMustLowerModifiedFlagIfSuccessful(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusiveAndTakeWriteLock(pageRef);
        long stamp = PageMetadata.unlockWriteAndTryTakeFlushLock(pageRef);
        assertTrue(PageMetadata.isModified(pageRef));
        PageMetadata.unlockFlush(pageRef, stamp, true);
        assertFalse(PageMetadata.isModified(pageRef));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void unlockWriteAndTryTakeFlushLockAndThenUnlockFlushMustNotLowerModifiedFlagIfFailed(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusiveAndTakeWriteLock(pageRef);
        long stamp = PageMetadata.unlockWriteAndTryTakeFlushLock(pageRef);
        assertTrue(PageMetadata.isModified(pageRef));
        PageMetadata.unlockFlush(pageRef, stamp, false);
        assertTrue(PageMetadata.isModified(pageRef));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void unlockWriteAndTryTakeFlushLockAndThenUnlockFlushWithOverlappingWriterMustNotLowerModifiedFlag(
            int pageId) {
        init(pageId);

        PageMetadata.unlockExclusiveAndTakeWriteLock(pageRef);
        long stamp = PageMetadata.unlockWriteAndTryTakeFlushLock(pageRef); // one flush lock
        assertThat(stamp).isNotEqualTo(0L);
        assertTrue(PageMetadata.isModified(pageRef));
        assertTrue(PageMetadata.tryWriteLock(pageRef, singleWriter)); // one flush and one write lock
        PageMetadata.unlockFlush(pageRef, stamp, true); // flush is successful, but have one overlapping writer
        PageMetadata.unlockWrite(pageRef); // no more locks, but a writer started within flush section ...
        assertTrue(PageMetadata.isModified(pageRef)); // ... and overlapped unlockFlush, so it's still modified
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void unlockWriteAndTryTakeFlushLockAndThenUnlockFlushWithContainedWriterMustNotLowerModifiedFlag(
            int pageId) {
        init(pageId);

        PageMetadata.unlockExclusiveAndTakeWriteLock(pageRef);
        long stamp = PageMetadata.unlockWriteAndTryTakeFlushLock(pageRef); // one flush lock
        assertThat(stamp).isNotEqualTo(0L);
        assertTrue(PageMetadata.isModified(pageRef));
        assertTrue(PageMetadata.tryWriteLock(pageRef, singleWriter)); // one flush and one write lock
        PageMetadata.unlockWrite(pageRef); // back to one flush lock
        PageMetadata.unlockFlush(pageRef, stamp, true); // flush is successful, but had one overlapping writer
        assertTrue(PageMetadata.isModified(pageRef)); // so it's still modified
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void unlockWriteAndTryTakeFlushLockThatSucceedsMustPreventOverlappingExclusiveLock(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusiveAndTakeWriteLock(pageRef);
        assertFalse(PageMetadata.tryExclusiveLock(pageRef));
        long stamp = PageMetadata.unlockWriteAndTryTakeFlushLock(pageRef);
        assertFalse(PageMetadata.tryExclusiveLock(pageRef));
        PageMetadata.unlockFlush(pageRef, stamp, true);
        assertTrue(PageMetadata.tryExclusiveLock(pageRef));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void unlockWriteAndTryTakeFlushLockThatFailsMustPreventOverlappingExclusiveLock(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusiveAndTakeWriteLock(pageRef);
        assertFalse(PageMetadata.tryExclusiveLock(pageRef));
        long stamp = PageMetadata.unlockWriteAndTryTakeFlushLock(pageRef);
        assertFalse(PageMetadata.tryExclusiveLock(pageRef));
        PageMetadata.unlockFlush(pageRef, stamp, false);
        assertTrue(PageMetadata.tryExclusiveLock(pageRef));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void unlockWriteAndTryTakeFlushLockThatSucceedsMustPreventOverlappingFlushLock(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusiveAndTakeWriteLock(pageRef);
        long stamp = PageMetadata.unlockWriteAndTryTakeFlushLock(pageRef);
        assertThat(PageMetadata.tryFlushLock(pageRef)).isEqualTo(0L);
        PageMetadata.unlockFlush(pageRef, stamp, true);
        assertThat(PageMetadata.tryFlushLock(pageRef)).isNotEqualTo(0L);
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void unlockWriteAndTryTakeFlushLockThatFailsMustPreventOverlappingFlushLock(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusiveAndTakeWriteLock(pageRef);
        long stamp = PageMetadata.unlockWriteAndTryTakeFlushLock(pageRef);
        assertThat(PageMetadata.tryFlushLock(pageRef)).isEqualTo(0L);
        PageMetadata.unlockFlush(pageRef, stamp, false);
        assertThat(PageMetadata.tryFlushLock(pageRef)).isNotEqualTo(0L);
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void unlockWriteAndTryTakeFlushLockMustNotInvalidateReadersOverlappingWithFlushLock(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusiveAndTakeWriteLock(pageRef);
        long flushStamp = PageMetadata.unlockWriteAndTryTakeFlushLock(pageRef);
        long readStamp = PageMetadata.tryOptimisticReadLock(pageRef);
        assertTrue(PageMetadata.validateReadLock(pageRef, readStamp));
        PageMetadata.unlockFlush(pageRef, flushStamp, true);
        assertTrue(PageMetadata.validateReadLock(pageRef, readStamp));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    void clearBindingResetPageHorizon(int pageId) {
        init(pageId);
        PageMetadata.setPageHorizon(pageRef, 42);

        PageMetadata.clearBinding(pageRef);
        assertEquals(0, PageMetadata.getPageHorizon(pageRef));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void unlockWriteAndTryTakeFlushLockMustInvalidateReadersOverlappingWithWriteLock(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusiveAndTakeWriteLock(pageRef);
        long readStamp = PageMetadata.tryOptimisticReadLock(pageRef);
        long flushStamp = PageMetadata.unlockWriteAndTryTakeFlushLock(pageRef);
        assertFalse(PageMetadata.validateReadLock(pageRef, readStamp));
        PageMetadata.unlockFlush(pageRef, flushStamp, true);
        assertFalse(PageMetadata.validateReadLock(pageRef, readStamp));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void addressesMustBeZeroBeforeInitialisation(int pageId) {
        init(pageId);

        assertThat(PageMetadata.getAddress(pageRef)).isEqualTo(0L);
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void addressMustNotBeZeroAfterInitialisation(int pageId) {
        init(pageId);

        pageMetadata.ensurePageAllocated(pageRef);
        assertThat(PageMetadata.getAddress(pageRef)).isNotEqualTo(0L);
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void usageCounterMustBeZeroByDefault(int pageId) {
        init(pageId);

        assertTrue(PageMetadata.decrementUsage(pageRef));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void usageCounterMustGoUpToFour(int pageId) {
        init(pageId);

        PageMetadata.incrementUsage(pageRef);
        PageMetadata.incrementUsage(pageRef);
        PageMetadata.incrementUsage(pageRef);
        PageMetadata.incrementUsage(pageRef);
        assertFalse(PageMetadata.decrementUsage(pageRef));
        assertFalse(PageMetadata.decrementUsage(pageRef));
        assertFalse(PageMetadata.decrementUsage(pageRef));
        assertTrue(PageMetadata.decrementUsage(pageRef));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void usageCounterMustTruncateAtFour(int pageId) {
        init(pageId);

        PageMetadata.incrementUsage(pageRef);
        PageMetadata.incrementUsage(pageRef);
        PageMetadata.incrementUsage(pageRef);
        PageMetadata.incrementUsage(pageRef);
        PageMetadata.incrementUsage(pageRef);
        assertFalse(PageMetadata.decrementUsage(pageRef));
        assertFalse(PageMetadata.decrementUsage(pageRef));
        assertFalse(PageMetadata.decrementUsage(pageRef));
        assertTrue(PageMetadata.decrementUsage(pageRef));
        assertTrue(PageMetadata.decrementUsage(pageRef));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void incrementingUsageCounterMustNotInterfereWithAdjacentUsageCounters(int pageId) {
        init(pageId);

        PageMetadata.incrementUsage(pageRef);
        PageMetadata.incrementUsage(pageRef);
        assertTrue(PageMetadata.decrementUsage(prevPageRef));
        assertTrue(PageMetadata.decrementUsage(nextPageRef));
        assertFalse(PageMetadata.decrementUsage(pageRef));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void decrementingUsageCounterMustNotInterfereWithAdjacentUsageCounters(int pageId) {
        init(pageId);

        for (int id : pageIds) {
            long ref = pageMetadata.deref(id);
            PageMetadata.incrementUsage(ref);
            PageMetadata.incrementUsage(ref);
        }

        assertFalse(PageMetadata.decrementUsage(pageRef));
        assertTrue(PageMetadata.decrementUsage(pageRef));
        assertFalse(PageMetadata.decrementUsage(prevPageRef));
        assertFalse(PageMetadata.decrementUsage(nextPageRef));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void filePageIdIsUnboundByDefault(int pageId) {
        init(pageId);

        assertThat(PageMetadata.getFilePageId(pageRef)).isEqualTo(PageCursor.UNBOUND_PAGE_ID);
    }

    // xxx ---[ Page fault tests ]---

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void faultMustThrowWithoutExclusiveLock(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        pageMetadata.ensurePageAllocated(pageRef);
        assertThrows(
                IllegalStateException.class,
                () -> MuninnPagedFile.validatePageRefAndSetFilePageId(pageRef, DUMMY_SWAPPER, (short) 0, 0));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void faultMustReadIntoPage(int pageId) throws Exception {
        init(pageId);

        byte pageByteContents = (byte) 0xF7;
        short swapperId = 1;
        long filePageId = 2;
        PageSwapper swapper = new DummyPageSwapper("some file", pageSize) {
            @Override
            public long read(long fpId, long bufferAddress) throws IOException {
                if (fpId == filePageId) {
                    UnsafeUtil.setMemory(bufferAddress, filePageSize, pageByteContents);
                    return filePageSize;
                }
                throw new IOException("Did not expect this file page id = " + fpId);
            }
        };
        pageMetadata.ensurePageAllocated(pageRef);
        MuninnPageCursor.fault(pageRef, swapper, swapperId, filePageId, PinPageFaultEvent.NULL);

        long address = PageMetadata.getAddress(pageRef);
        assertThat(address).isNotEqualTo(0L);
        for (int i = 0; i < pageSize; i++) {
            int index = i;
            byte actualByteContents = UnsafeUtil.getByte(address + index);
            assertEquals(
                    pageByteContents,
                    actualByteContents,
                    () -> format(
                            "Page contents where different at address %x + %s, expected %x but was %x",
                            address, index, pageByteContents, actualByteContents));
        }
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void pageMustBeLoadedAndBoundAfterFault(int pageId) throws Exception {
        init(pageId);

        // exclusive lock implied by constructor
        int swapperId = 1;
        long filePageId = 42;
        pageMetadata.ensurePageAllocated(pageRef);
        MuninnPagedFile.validatePageRefAndSetFilePageId(pageRef, DUMMY_SWAPPER, swapperId, filePageId);
        MuninnPageCursor.fault(pageRef, DUMMY_SWAPPER, swapperId, filePageId, PinPageFaultEvent.NULL);
        assertThat(PageMetadata.getFilePageId(pageRef)).isEqualTo(filePageId);
        assertThat(PageMetadata.getSwapperId(pageRef)).isEqualTo(swapperId);
        assertTrue(PageMetadata.isLoaded(pageRef));
        assertTrue(PageMetadata.isBoundTo(pageRef, swapperId, filePageId));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void pageWith5BytesFilePageIdMustBeLoadedAndBoundAfterFault(int pageId) throws Exception {
        init(pageId);

        // exclusive lock implied by constructor
        int swapperId = 12;
        long filePageId = Integer.MAX_VALUE + 1L;
        pageMetadata.ensurePageAllocated(pageRef);
        MuninnPagedFile.validatePageRefAndSetFilePageId(pageRef, DUMMY_SWAPPER, swapperId, filePageId);
        MuninnPageCursor.fault(pageRef, DUMMY_SWAPPER, swapperId, filePageId, PinPageFaultEvent.NULL);
        assertThat(PageMetadata.getFilePageId(pageRef)).isEqualTo(filePageId);
        assertThat(PageMetadata.getSwapperId(pageRef)).isEqualTo(swapperId);
        assertTrue(PageMetadata.isLoaded(pageRef));
        assertTrue(PageMetadata.isBoundTo(pageRef, swapperId, filePageId));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void pageMustBeLoadedAndNotBoundIfFaultThrows(int pageId) {
        init(pageId);

        // exclusive lock implied by constructor
        PageSwapper swapper = new DummyPageSwapper("file", pageSize) {
            @Override
            public long read(long filePageId, long bufferAddress) throws IOException {
                throw new IOException("boo");
            }
        };
        int swapperId = 1;
        long filePageId = 42;
        pageMetadata.ensurePageAllocated(pageRef);
        assertThatThrownBy(() -> {
                    MuninnPagedFile.validatePageRefAndSetFilePageId(pageRef, swapper, swapperId, filePageId);
                    MuninnPageCursor.fault(pageRef, swapper, swapperId, filePageId, PinPageFaultEvent.NULL);
                })
                .isInstanceOf(IOException.class)
                .hasMessage("boo");
        assertThat(PageMetadata.getFilePageId(pageRef)).isEqualTo(filePageId);
        assertThat(PageMetadata.getSwapperId(pageRef)).isEqualTo(0); // 0 means not bound
        assertTrue(PageMetadata.isLoaded(pageRef));
        assertFalse(PageMetadata.isBoundTo(pageRef, swapperId, filePageId));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void faultMustThrowIfPageIsAlreadyBound(int pageId) throws Exception {
        init(pageId);

        // exclusive lock implied by constructor
        short swapperId = 1;
        long filePageId = 42;
        pageMetadata.ensurePageAllocated(pageRef);
        MuninnPagedFile.validatePageRefAndSetFilePageId(pageRef, DUMMY_SWAPPER, swapperId, filePageId);
        MuninnPageCursor.fault(pageRef, DUMMY_SWAPPER, swapperId, filePageId, PinPageFaultEvent.NULL);

        assertThrows(
                IllegalStateException.class,
                () -> MuninnPagedFile.validatePageRefAndSetFilePageId(pageRef, DUMMY_SWAPPER, swapperId, filePageId));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void faultMustThrowIfPageIsLoadedButNotBound(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        short swapperId = 1;
        long filePageId = 42;
        doFailedFault(swapperId, filePageId);

        // After the failed page fault, the page is loaded but not bound.
        // We still can't fault into a loaded page, though.
        assertThrows(
                IllegalStateException.class,
                () -> MuninnPagedFile.validatePageRefAndSetFilePageId(pageRef, DUMMY_SWAPPER, swapperId, filePageId));
    }

    private void doFailedFault(short swapperId, long filePageId) {
        assertTrue(PageMetadata.tryExclusiveLock(pageRef));
        pageMetadata.ensurePageAllocated(pageRef);
        DummyPageSwapper swapper = new DummyPageSwapper("", pageSize) {
            @Override
            public long read(long filePageId, long bufferAddress) throws IOException {
                throw new IOException("boom");
            }
        };
        assertThatThrownBy(() -> {
                    MuninnPagedFile.validatePageRefAndSetFilePageId(pageRef, swapper, swapperId, filePageId);
                    MuninnPageCursor.fault(pageRef, swapper, swapperId, filePageId, PinPageFaultEvent.NULL);
                })
                .isInstanceOf(IOException.class)
                .hasMessage("boom");
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void faultMustPopulatePageFaultEvent(int pageId) throws Exception {
        init(pageId);

        // exclusive lock implied by constructor
        short swapperId = 1;
        long filePageId = 42;
        pageMetadata.ensurePageAllocated(pageRef);
        DummyPageSwapper swapper = new DummyPageSwapper("", pageSize) {
            @Override
            public long read(long filePageId, long bufferAddress) {
                return 333;
            }
        };
        StubPinPageFaultEvent event = new StubPinPageFaultEvent();
        MuninnPageCursor.fault(pageRef, swapper, swapperId, filePageId, event);
        assertThat(event.bytesRead).isEqualTo(333L);
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void unboundPageMustNotBeLoaded(int pageId) {
        init(pageId);

        assertFalse(PageMetadata.isLoaded(pageRef));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void unboundPageMustNotBeBoundToAnything(int pageId) {
        init(pageId);

        assertFalse(PageMetadata.isBoundTo(pageRef, (short) 0, 0));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void boundPagesAreNotBoundToOtherPagesWithSameSwapper(int pageId) throws Exception {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        long filePageId = 42;
        short swapperId = 2;
        doFault(swapperId, filePageId);

        assertTrue(PageMetadata.isBoundTo(pageRef, swapperId, filePageId));
        assertFalse(PageMetadata.isBoundTo(pageRef, swapperId, filePageId + 1));
        assertFalse(PageMetadata.isBoundTo(pageRef, swapperId, filePageId - 1));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void boundPagesAreNotBoundToOtherPagesWithSameFilePageId(int pageId) throws Exception {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        short swapperId = 2;
        doFault(swapperId, 42);

        assertTrue(PageMetadata.isBoundTo(pageRef, swapperId, 42));
        assertFalse(PageMetadata.isBoundTo(pageRef, (short) (swapperId + 1), 42));
        assertFalse(PageMetadata.isBoundTo(pageRef, (short) (swapperId - 1), 42));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void faultMustNotInterfereWithAdjacentPages(int pageId) throws Exception {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        doFault((short) 1, 42);

        assertFalse(PageMetadata.isLoaded(prevPageRef));
        assertFalse(PageMetadata.isLoaded(nextPageRef));
        assertFalse(PageMetadata.isBoundTo(prevPageRef, (short) 1, 42));
        assertFalse(PageMetadata.isBoundTo(prevPageRef, (short) 0, 0));
        assertFalse(PageMetadata.isBoundTo(nextPageRef, (short) 1, 42));
        assertFalse(PageMetadata.isBoundTo(nextPageRef, (short) 0, 0));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void reportWriteLockStatus(int pageId) {
        init(pageId);

        assertFalse(PageMetadata.isWriteLocked(pageRef));
        PageMetadata.unlockExclusive(pageRef);

        assertFalse(PageMetadata.isWriteLocked(pageRef));
        assertTrue(PageMetadata.tryWriteLock(pageRef, singleWriter));

        if (!singleWriter) {
            for (int i = 0; i < 11; i++) {
                assertTrue(PageMetadata.tryWriteLock(pageRef, singleWriter));
                assertTrue(PageMetadata.isWriteLocked(pageRef));
            }

            for (int i = 0; i < 11; i++) {
                PageMetadata.unlockWrite(pageRef);
                assertTrue(PageMetadata.isWriteLocked(pageRef));
            }
        }

        PageMetadata.unlockWrite(pageRef);
        assertFalse(PageMetadata.isWriteLocked(pageRef));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void failedFaultMustNotInterfereWithAdjacentPages(int pageId) {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        doFailedFault((short) 1, 42);

        assertFalse(PageMetadata.isLoaded(prevPageRef));
        assertFalse(PageMetadata.isLoaded(nextPageRef));
        assertFalse(PageMetadata.isBoundTo(prevPageRef, (short) 1, 42));
        assertFalse(PageMetadata.isBoundTo(prevPageRef, (short) 0, 0));
        assertFalse(PageMetadata.isBoundTo(nextPageRef, (short) 1, 42));
        assertFalse(PageMetadata.isBoundTo(nextPageRef, (short) 0, 0));
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void exclusiveLockMustStillBeHeldAfterFault(int pageId) throws Exception {
        init(pageId);

        PageMetadata.unlockExclusive(pageRef);
        doFault((short) 1, 42);
        PageMetadata.unlockExclusive(pageRef); // will throw if lock is not held
    }

    @ParameterizedTest(name = "pageRef = {0}")
    @MethodSource("argumentsProvider")
    public void failToSetHigherThenSupportedFilePageIdOnFault(int pageId) {
        init(pageId);

        assertThrows(IllegalArgumentException.class, () -> {
            PageMetadata.unlockExclusive(pageRef);
            short swapperId = 2;
            doFault(swapperId, Long.MAX_VALUE);
        });
    }

    private void doFault(int swapperId, long filePageId) throws IOException {
        assertTrue(PageMetadata.tryExclusiveLock(pageRef));
        MuninnPagedFile.validatePageRefAndSetFilePageId(pageRef, DUMMY_SWAPPER, swapperId, filePageId);
        pageMetadata.ensurePageAllocated(pageRef);
        MuninnPageCursor.fault(pageRef, DUMMY_SWAPPER, swapperId, filePageId, PinPageFaultEvent.NULL);
    }
}
