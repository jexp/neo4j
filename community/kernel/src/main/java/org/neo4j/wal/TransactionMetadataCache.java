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
package org.neo4j.wal;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.eclipse.collections.api.factory.primitive.LongObjectMaps;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.neo4j.collection.PrimitiveLongArrayQueue;

/**
 * A simple cache that keeps the last {@value #DEFAULT_METADATA_CACHE_SIZE} added transaction metadata records.
 * <p>
 * It behaves like a cache with Least recent added eviction policy.
 * <p>
 * Notes about the concurrency:
 * One of the design goal of this cache is low memory footprint, therefore it uses simple collections
 * guarded by a read-write lock. Concurrent collections like ConcurrentHashMap could have been used for improved
 * concurrency, but they would take more memory. The concurrent use of this cache should not be anywhere near
 * a simple read-write lock vs. concurrent collections making any performance difference.
 */
public class TransactionMetadataCache {
    private static final int DEFAULT_METADATA_CACHE_SIZE = 10_000;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final MutableLongObjectMap<LogPosition> cacheMap;
    private final PrimitiveLongArrayQueue writeOrderQueue;

    public TransactionMetadataCache() {
        cacheMap = LongObjectMaps.mutable.ofInitialCapacity(DEFAULT_METADATA_CACHE_SIZE);
        writeOrderQueue = new PrimitiveLongArrayQueue(queueCapacity());
    }

    // PrimitiveLongArrayQueue allows only capacity that is power of 2
    private static int queueCapacity() {
        return Integer.highestOneBit(DEFAULT_METADATA_CACHE_SIZE - 1) << 1;
    }

    public void clear() {
        lock.writeLock().lock();
        try {
            cacheMap.clear();
            writeOrderQueue.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public TransactionMetadata getTransactionMetadata(long appendIndex) {
        lock.readLock().lock();
        try {
            var logPosition = cacheMap.get(appendIndex);
            if (logPosition == null) {
                return null;
            }

            return new TransactionMetadata(logPosition);
        } finally {
            lock.readLock().unlock();
        }
    }

    public void cacheTransactionMetadata(long appendIndex, LogPosition position) {
        if (LogPosition.UNSPECIFIED == position) {
            throw new IllegalArgumentException("Metadata cache only supports specified log positions.");
        }

        lock.writeLock().lock();
        try {
            if (cacheMap.size() == DEFAULT_METADATA_CACHE_SIZE) {
                var victim = writeOrderQueue.dequeue();
                cacheMap.remove(victim);
            }
            writeOrderQueue.enqueue(appendIndex);
            cacheMap.put(appendIndex, position);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public record TransactionMetadata(LogPosition startPosition) {}
}
