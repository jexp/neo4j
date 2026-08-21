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
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.DatabaseFlushEvent;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.async.AsyncFlushCompletion;

class AsyncCheckpointCompletionHandlerTest {

    @Test
    void unlockSinglePageFlushLockOnCompletion() {
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

            var completionHandler = new AsyncCheckpointCompletionHandler(DatabaseFlushEvent.NULL);
            completionHandler.handleCompletion(
                    new AsyncBlockAccessorWithResult(new AsyncVectorIOData(pageRef, flushLock)), 4, 0);
            // page is not modified anymore since flush lock was releases with success flag
            assertThat(PageMetadata.isModified(pageRef)).isFalse();
            // can flush lock again
            long flushLockAfterHandle = PageMetadata.tryFlushLock(pageRef);
            assertThat(flushLockAfterHandle).isNotZero();
        }
    }

    @Test
    void unlockSeveralPageFlushLockOnCompletion() {
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

            var completionHandler = new AsyncCheckpointCompletionHandler(DatabaseFlushEvent.NULL);
            completionHandler.handleCompletion(
                    new AsyncBlockAccessorWithResult(new AsyncVectorIOData(
                            new long[] {pageRef1, pageRef2, pageRef3},
                            new long[] {flushLock1, flushLock2, flushLock3},
                            3)),
                    4,
                    0);

            // pages are not modified anymore since flush lock was releases with success flag
            assertThat(PageMetadata.isModified(pageRef1)).isFalse();
            assertThat(PageMetadata.isModified(pageRef2)).isFalse();
            assertThat(PageMetadata.isModified(pageRef3)).isFalse();

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

                // flush locks
                long flushLock1 = PageMetadata.tryFlushLock(pageRef1);
                long flushLock2 = PageMetadata.tryFlushLock(pageRef2);
                long flushLock3 = PageMetadata.tryFlushLock(pageRef3);

                assertThat(flushLock1).isNotZero();
                assertThat(flushLock2).isNotZero();
                assertThat(flushLock3).isNotZero();

                var completionHandler = new AsyncCheckpointCompletionHandler(databaseFlush);
                completionHandler.handleCompletion(
                        new AsyncBlockAccessorWithResult(new AsyncVectorIOData(
                                new long[] {pageRef1, pageRef2}, new long[] {flushLock1, flushLock2}, 1)),
                        4,
                        0);

                completionHandler.handleCompletion(
                        new AsyncBlockAccessorWithResult(
                                new AsyncVectorIOData(new long[] {pageRef3}, new long[] {flushLock3}, 1)),
                        4,
                        0);

                try (AsyncFlushCompletion asyncFlushCompletion = databaseFlush.asyncFlushCompletion()) {
                    assertThat(asyncFlushCompletion.ioPerformed()).isEqualTo(2);
                    assertThat(asyncFlushCompletion.pagesFlushed()).isEqualTo(3);
                    assertThat(databaseFlush.pagesFlushed()).isZero();
                    assertThat(databaseFlush.ioPerformed()).isZero();
                }
            }

            databaseFlush.close();

            assertThat(databaseFlush.pagesFlushed()).isEqualTo(3);
            assertThat(databaseFlush.ioPerformed()).isEqualTo(2);
            assertThat(defaultPageCacheTracer.bytesWritten()).isEqualTo(3 * PageCache.PAGE_SIZE);
        }
    }
}
