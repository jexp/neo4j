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

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.io.ByteUnit.kibiBytes;

import org.junit.jupiter.api.Test;

class SharedMultiRootLfuCacheTest {
    @Test
    void shouldKeepNamespacesIsolatedForTheSameKey() {
        // given
        var shared = new SharedMultiRootLfuCache(kibiBytes(1));
        RootMappingCache<Long, String> a = shared.create("test");
        RootMappingCache<Long, String> b = shared.create("test");

        // when
        a.put(1L, "a-value");
        b.put(1L, "b-value");

        // then
        assertThat(a.compute(1L, (k, v) -> v)).isEqualTo("a-value");
        assertThat(b.compute(1L, (k, v) -> v)).isEqualTo("b-value");

        // and when
        a.remove(1L);

        // then
        assertThat(a.compute(1L, (k, v) -> v)).isNull();
        assertThat(b.compute(1L, (k, v) -> v)).isEqualTo("b-value");
    }

    @Test
    void clearShouldOnlyEvictOwnNamespace() {
        // given
        var shared = new SharedMultiRootLfuCache(kibiBytes(1));
        RootMappingCache<Long, String> a = shared.create("test");
        RootMappingCache<Long, String> b = shared.create("test");
        a.put(1L, "a1");
        a.put(2L, "a2");
        b.put(1L, "b1");

        // when
        a.clear();

        // then
        assertThat(a.compute(1L, (k, v) -> v)).isNull();
        assertThat(a.compute(2L, (k, v) -> v)).isNull();
        assertThat(b.compute(1L, (k, v) -> v)).isEqualTo("b1");
    }

    @Test
    void putIfAbsentAndComputeIfAbsentShouldRespectNamespace() {
        // given
        var shared = new SharedMultiRootLfuCache(kibiBytes(1));
        RootMappingCache<Long, String> a = shared.create("test");
        RootMappingCache<Long, String> b = shared.create("test");

        // when/then
        assertThat(a.putIfAbsent(1L, "a1")).isNull();
        assertThat(a.putIfAbsent(1L, "a1-again")).isEqualTo("a1");
        // Same key, different namespace -> absent, so the value is installed
        assertThat(b.putIfAbsent(1L, "b1")).isNull();

        assertThat(a.computeIfAbsent(1L, k -> "unused")).isEqualTo("a1");
        assertThat(b.computeIfAbsent(2L, k -> "b2")).isEqualTo("b2");
        assertThat(a.compute(2L, (k, v) -> v)).isNull();
    }

    @Test
    void shouldBoundTotalSizeAcrossNamespaces() {
        // given
        long maxMemory = kibiBytes(1);
        var shared = new SharedMultiRootLfuCache(maxMemory);
        RootMappingCache<Long, String> a = shared.create("test");
        for (long i = 0; i < 10_000; i++) {
            a.put(i, "a" + i);
        }
        long estimatedSizeA = shared.estimatedSize();

        // when
        RootMappingCache<Long, String> b = shared.create("test");
        for (long i = 0; i < 10_000; i++) {
            b.put(i, "a" + i);
        }

        // then
        assertThat(shared.estimatedSize()).isLessThanOrEqualTo(estimatedSizeA);
    }
}
