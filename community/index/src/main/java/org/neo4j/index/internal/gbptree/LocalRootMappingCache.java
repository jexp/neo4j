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

import java.util.function.BiFunction;
import java.util.function.Function;
import org.neo4j.internal.helpers.collection.LfuCache;

/**
 * A {@link RootMappingCache} backed by a private {@link LfuCache}, bounded independently of any other cache.
 */
class LocalRootMappingCache<K, V> implements RootMappingCache<K, V> {
    private final LfuCache<K, V> cache;

    LocalRootMappingCache(String name, int maxSize) {
        this.cache = new LfuCache<>(name, maxSize);
    }

    @Override
    public V putIfAbsent(K key, V value) {
        return cache.putIfAbsent(key, value);
    }

    @Override
    public V compute(K key, BiFunction<K, V, V> remappingFunction) {
        return cache.compute(key, remappingFunction);
    }

    @Override
    public V computeIfAbsent(K key, Function<K, V> mappingFunction) {
        return cache.computeIfAbsent(key, mappingFunction);
    }

    @Override
    public void put(K key, V value) {
        cache.put(key, value);
    }

    @Override
    public void remove(K key) {
        cache.remove(key);
    }

    @Override
    public void clear() {
        // No need to spend time clearing the cache if it isn't shared with others.
    }
}
