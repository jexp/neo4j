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

import static java.lang.Math.clamp;

/**
 * Factory for creating the {@link RootMappingCache} that a {@link MultiRootLayer} uses.
 */
public interface RootMappingCacheFactory {
    <K, V> RootMappingCache<K, V> create(String name);

    static int maxNumberOfCachedItems(long maxMemory, int additionalItemOverhead) {
        int itemSize = 16 /*obj.overhead*/
                + 16 /*obj.fields*/
                + 16 /*inner root instance*/
                + 8 /* cache references*/
                + additionalItemOverhead;
        return clamp(maxMemory / itemSize, itemSize * 100, Integer.MAX_VALUE);
    }

    /**
     * Each tree gets its own private cache bounded to {@code maxSize} entries.
     */
    class LocalRootMappingCacheFactory implements RootMappingCacheFactory {
        private final long maxMemory;

        public LocalRootMappingCacheFactory(long maxMemory) {
            this.maxMemory = maxMemory;
        }

        @Override
        public <K, V> RootMappingCache<K, V> create(String name) {
            return new LocalRootMappingCache<>(name, maxNumberOfCachedItems(maxMemory, 0));
        }
    }
}
