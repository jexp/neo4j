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

/**
 * Cache of the root mappings that a {@link MultiRootLayer} maintains, i.e. mappings from user-defined root key to the
 * data tree root. This interface is derived directly from {@link org.neo4j.internal.helpers.collection.LfuCache}.
 *
 * @param <K> root key type.
 * @param <V> cached value type.
 */
public interface RootMappingCache<K, V> {
    V putIfAbsent(K key, V value);

    V compute(K key, BiFunction<K, V, V> remappingFunction);

    V computeIfAbsent(K key, Function<K, V> mappingFunction);

    void put(K key, V value);

    void remove(K key);

    /**
     * Removes all entries belonging to this cache instance. For a shared backing pool this only evicts this
     * instance's namespace, leaving other instances' entries intact.
     */
    void clear();
}
