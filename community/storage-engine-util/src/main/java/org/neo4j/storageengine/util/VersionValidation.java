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
package org.neo4j.storageengine.util;

import static org.neo4j.lock.ResourceType.PAGE;

import java.io.IOException;
import org.neo4j.io.layout.DatabaseFile;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.VersionContext;
import org.neo4j.kernel.impl.monitoring.TransactionMonitor;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceLocker;
import org.neo4j.storageengine.api.txstate.validation.TransactionConflictException;

public class VersionValidation {
    // This number divides the 64 bits of a long into 48 bits for the page id and 16 bits for the store id.
    // 48 bits is enough to encode 2^48-1= 281474976710655 as the highest page id.
    // 16 bits is enough to encode 2^16-1= 65535 as the highest allowed store id.
    public static final short PAGE_ID_BITS = 48;
    private static final long MAX_PAGE_ID = (1L << PAGE_ID_BITS) - 1;
    private static final long MAX_STORE_ID = (1L << (Long.SIZE - PAGE_ID_BITS)) - 1;

    public static final long PAGE_ID_MASK = MAX_PAGE_ID;

    public static void validatePageVersion(
            DatabaseFile databaseFile,
            long pageId,
            VersionContext versionContext,
            PageCursor pageCursor,
            long storeId,
            boolean failFast,
            ResourceLocker validationLockClient,
            TransactionMonitor transactionMonitor,
            LockTracer lockTracer)
            throws IOException {
        long id = lockPageId(pageId, storeId);
        if (failFast) {
            if (!validationLockClient.tryExclusiveLock(PAGE, id)) {
                throw TransactionConflictException.transactionConflict(databaseFile, pageId);
            }
        } else {
            validationLockClient.acquireExclusive(lockTracer, PAGE, id);
        }

        if (pageCursor.next(pageId)) {
            if (versionContext.invisibleHeadObserved()) {
                transactionMonitor.transactionValidationFailure(databaseFile);
                throw TransactionConflictException.transactionConflict(databaseFile, versionContext, pageId);
            }
        }
    }

    public static long lockPageId(long pageId, long storeId) {
        assert pageId <= MAX_PAGE_ID;
        assert storeId <= MAX_STORE_ID;

        return pageId | (storeId << PAGE_ID_BITS);
    }
}
