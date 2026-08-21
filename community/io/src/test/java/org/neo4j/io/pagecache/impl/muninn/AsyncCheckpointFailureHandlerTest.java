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

import org.junit.jupiter.api.Test;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.async.AsyncVectorIOData;
import org.neo4j.io.pagecache.tracing.DatabaseFlushEvent;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.async.AsyncFlushFailure;

class AsyncCheckpointFailureHandlerTest {

    @Test
    void unlockSinglePageFlushLockOnFailure() {
        int pageSize = (int) ByteUnit.kibiBytes(8);
        int pages = 10;

        try (var pageMetadata = new PageMetadata(pages, pageSize)) {
            long pageRef = pageMetadata.deref(0);
            PageMetadata.unlockExclusive(pageRef);

            // page is modified
            assertThat(PageMetadata.tryWriteLock(pageRef, false)).isTrue();
            PageMetadata.unlockWrite(pageRef);
            assertThat(PageMetadata.isModified(pageRef)).isTrue();

            long flushLock = PageMetadata.tryFlushLock(pageRef);
            assertThat(flushLock).isNotZero();

            var failureHandler = new AsyncCheckpointFailureHandler(DatabaseFlushEvent.NULL);
            failureHandler.handleFailure(
                    new AsyncBlockAccessorWithResult(new AsyncVectorIOData(pageRef, flushLock)),
                    4,
                    0,
                    "bad news everyone");
            // page is still modified since flush lock was releases with false as success
            assertThat(PageMetadata.isModified(pageRef)).isTrue();

            // can flush lock again
            long flushLockAfterHandle = PageMetadata.tryFlushLock(pageRef);
            assertThat(flushLockAfterHandle).isNotZero();
        }
    }

    @Test
    void unlockSeveralPageFlushLockOnFailure() {
        int pageSize = (int) ByteUnit.kibiBytes(8);
        int pages = 10;

        try (var pageMetadata = new PageMetadata(pages, pageSize)) {
            long pageRef1 = pageMetadata.deref(0);
            long pageRef2 = pageMetadata.deref(1);
            long pageRef3 = pageMetadata.deref(2);

            PageMetadata.unlockExclusive(pageRef1);
            PageMetadata.unlockExclusive(pageRef2);
            PageMetadata.unlockExclusive(pageRef3);

            // modify pages
            assertThat(PageMetadata.tryWriteLock(pageRef1, false)).isTrue();
            PageMetadata.unlockWrite(pageRef1);
            assertThat(PageMetadata.isModified(pageRef1)).isTrue();

            assertThat(PageMetadata.tryWriteLock(pageRef2, false)).isTrue();
            PageMetadata.unlockWrite(pageRef2);
            assertThat(PageMetadata.isModified(pageRef2)).isTrue();

            assertThat(PageMetadata.tryWriteLock(pageRef3, false)).isTrue();
            PageMetadata.unlockWrite(pageRef3);
            assertThat(PageMetadata.isModified(pageRef3)).isTrue();

            // flush locks
            long flushLock1 = PageMetadata.tryFlushLock(pageRef1);
            long flushLock2 = PageMetadata.tryFlushLock(pageRef2);
            long flushLock3 = PageMetadata.tryFlushLock(pageRef3);

            assertThat(flushLock1).isNotZero();
            assertThat(flushLock2).isNotZero();
            assertThat(flushLock3).isNotZero();

            var failureHandler = new AsyncCheckpointFailureHandler(DatabaseFlushEvent.NULL);
            failureHandler.handleFailure(
                    new AsyncBlockAccessorWithResult(new AsyncVectorIOData(
                            new long[] {pageRef1, pageRef2, pageRef3},
                            new long[] {flushLock1, flushLock2, flushLock3},
                            3)),
                    4,
                    0,
                    "bad news everyone");

            // pages are still modified anymore since flush lock was releases with false as success flag
            assertThat(PageMetadata.isModified(pageRef1)).isTrue();
            assertThat(PageMetadata.isModified(pageRef2)).isTrue();
            assertThat(PageMetadata.isModified(pageRef3)).isTrue();

            // can flush lock again
            long flushLockAfterHandle1 = PageMetadata.tryFlushLock(pageRef1);
            assertThat(flushLockAfterHandle1).isNotZero();
            long flushLockAfterHandle2 = PageMetadata.tryFlushLock(pageRef2);
            assertThat(flushLockAfterHandle2).isNotZero();
            long flushLockAfterHandle3 = PageMetadata.tryFlushLock(pageRef3);
            assertThat(flushLockAfterHandle3).isNotZero();
        }
    }

    @Test
    void reportEventsOnCompletion() {
        int pageSize = (int) ByteUnit.kibiBytes(8);
        int pages = 10;

        DefaultPageCacheTracer defaultPageCacheTracer = new DefaultPageCacheTracer();
        try (DatabaseFlushEvent databaseFlush = defaultPageCacheTracer.beginDatabaseFlush()) {
            try (var pageMetadata = new PageMetadata(pages, pageSize)) {
                long pageRef1 = pageMetadata.deref(0);
                long pageRef2 = pageMetadata.deref(1);
                long pageRef3 = pageMetadata.deref(2);

                PageMetadata.unlockExclusive(pageRef1);
                PageMetadata.unlockExclusive(pageRef2);
                PageMetadata.unlockExclusive(pageRef3);

                // modify pages
                assertThat(PageMetadata.tryWriteLock(pageRef1, false)).isTrue();
                PageMetadata.unlockWrite(pageRef1);

                assertThat(PageMetadata.tryWriteLock(pageRef2, false)).isTrue();
                PageMetadata.unlockWrite(pageRef2);

                assertThat(PageMetadata.tryWriteLock(pageRef3, false)).isTrue();
                PageMetadata.unlockWrite(pageRef3);

                // flush locks
                long flushLock1 = PageMetadata.tryFlushLock(pageRef1);
                long flushLock2 = PageMetadata.tryFlushLock(pageRef2);
                long flushLock3 = PageMetadata.tryFlushLock(pageRef3);

                assertThat(flushLock1).isNotZero();
                assertThat(flushLock2).isNotZero();
                assertThat(flushLock3).isNotZero();

                var failureHandler = new AsyncCheckpointFailureHandler(databaseFlush);
                failureHandler.handleFailure(
                        new AsyncBlockAccessorWithResult(new AsyncVectorIOData(
                                new long[] {pageRef1, pageRef2}, new long[] {flushLock1, flushLock2}, 1)),
                        4,
                        0,
                        "bad news everyone");
                failureHandler.handleFailure(
                        new AsyncBlockAccessorWithResult(
                                new AsyncVectorIOData(new long[] {pageRef3}, new long[] {flushLock3}, 1)),
                        4,
                        0,
                        "bad news everyone");

                try (AsyncFlushFailure asyncFlushFailure = databaseFlush.asyncFlushFailure()) {
                    assertThat(asyncFlushFailure.ioPerformed()).isEqualTo(2);
                    assertThat(databaseFlush.ioPerformed()).isZero();
                }

                databaseFlush.close();
                assertThat(databaseFlush.ioPerformed()).isEqualTo(2);
                assertThat(defaultPageCacheTracer.asyncIoFailed()).isEqualTo(3);
            }
        }
    }
}
