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
package org.neo4j.index.internal.gbptree;

import static org.neo4j.index.internal.gbptree.RootMappingCacheFactory.maxNumberOfCachedItems;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.neo4j.internal.helpers.collection.LfuCache;
import org.neo4j.memory.HeapEstimator;

/**
 * A single bounded {@link LfuCache} shared by many {@code MultiRootGBPTree} root-mapping caches, registered as a
 * dbms-wide dependency so that every database's root cache draws from one pool instead of each getting its own.
 * This bounds the total memory used for root mappings regardless of the number of databases,
 * and lets Caffeine's frequency-based eviction self-balance the pool across databases: a busy database naturally keeps
 * more of its roots cached than an idle one.
 * <p>
 * Each {@link #create(String) create} call hands out a distinct namespace within the shared pool. Entries are
 * keyed by {@code (namespaceId, rootKey)}, so {@link RootMappingCache#clear()} only evicts that namespace's entries.
 * <p>
 * The pool is sized lazily by the first caller (first-caller-wins). Callers are expected to pass the same size, since
 * the intent is one dbms-wide max-size rather than a per-database one.
 */
public class SharedMultiRootLfuCache implements RootMappingCacheFactory {
    private static final int NAMESPACED_KEY_SHALLOW_SIZE =
            (int) HeapEstimator.shallowSizeOfInstance(NamespacedKey.class);

    private final AtomicInteger namespaceIds = new AtomicInteger();
    private final long maxMemory;
    private volatile LfuCache<NamespacedKey, Object> pool;

    public SharedMultiRootLfuCache(long maxMemory) {
        this.maxMemory = maxMemory;
    }

    @Override
    public <K, V> RootMappingCache<K, V> create(String name) {
        return new View<>(pool(name), namespaceIds.getAndIncrement());
    }

    private synchronized LfuCache<NamespacedKey, Object> pool(String name) {
        if (pool == null) {
            pool = new LfuCache<>(name, maxNumberOfCachedItems(maxMemory, NAMESPACED_KEY_SHALLOW_SIZE));
        }
        return pool;
    }

    /**
     * @return the estimated number of entries currently held across all namespaces in the shared pool, or {@code 0}
     * before the pool has been created by the first {@link #create(String)} call.
     */
    public long estimatedSize() {
        var currentPool = pool;
        return currentPool == null ? 0 : currentPool.estimatedSize();
    }

    private record NamespacedKey(int namespaceId, Object rootKey) {}

    private static final class View<K, V> implements RootMappingCache<K, V> {
        private final LfuCache<NamespacedKey, Object> pool;
        private final int namespaceId;

        View(LfuCache<NamespacedKey, Object> pool, int namespaceId) {
            this.pool = pool;
            this.namespaceId = namespaceId;
        }

        @Override
        public V putIfAbsent(K key, V value) {
            return cast(pool.putIfAbsent(key(key), value));
        }

        @Override
        public V compute(K key, BiFunction<K, V, V> remappingFunction) {
            return cast(
                    pool.compute(key(key), (namespacedKey, current) -> remappingFunction.apply(key, cast(current))));
        }

        @Override
        public V computeIfAbsent(K key, Function<K, V> mappingFunction) {
            return cast(pool.computeIfAbsent(key(key), namespacedKey -> mappingFunction.apply(key)));
        }

        @Override
        public void put(K key, V value) {
            pool.put(key(key), value);
        }

        @Override
        public void remove(K key) {
            pool.remove(key(key));
        }

        @Override
        public void clear() {
            // Evict only this namespace's entries. clear() is only called when a tree closes (not on a hot path), so a
            // scan of the shared pool is acceptable and avoids the bookkeeping of a per-namespace key index.
            List<NamespacedKey> mine = new ArrayList<>();
            for (NamespacedKey key : pool.keySet()) {
                if (key.namespaceId == namespaceId) {
                    mine.add(key);
                }
            }
            mine.forEach(pool::remove);
        }

        private NamespacedKey key(K key) {
            return new NamespacedKey(namespaceId, key);
        }

        @SuppressWarnings("unchecked")
        private V cast(Object value) {
            return (V) value;
        }
    }
}
