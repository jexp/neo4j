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
package org.neo4j.kernel.impl.transaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.neo4j.wal.LogPosition;
import org.neo4j.wal.TransactionMetadataCache;

class TransactionMetadataCacheTest {
    @Test
    void shouldReturnNullWhenMissingATxInTheCache() {
        // given
        final TransactionMetadataCache cache = new TransactionMetadataCache();

        // when
        final TransactionMetadataCache.TransactionMetadata metadata = cache.getTransactionMetadata(42);

        // then
        assertNull(metadata);
    }

    @Test
    void shouldReturnTheTxValueTIfInTheCached() {
        // given
        final TransactionMetadataCache cache = new TransactionMetadataCache();
        final LogPosition position = new LogPosition(3, 4);
        final int txId = 42;

        // when
        cache.cacheTransactionMetadata(txId, position);
        final TransactionMetadataCache.TransactionMetadata metadata = cache.getTransactionMetadata(txId);

        // then
        assertEquals(new TransactionMetadataCache.TransactionMetadata(position), metadata);
    }

    @Test
    void shouldThrowWhenCachingATxWithNegativeOffsetPosition() {
        var cache = new TransactionMetadataCache();
        assertThrows(IllegalArgumentException.class, () -> cache.cacheTransactionMetadata(42, LogPosition.UNSPECIFIED));
    }

    @Test
    void shouldClearTheCache() {
        // given
        final TransactionMetadataCache cache = new TransactionMetadataCache();
        final LogPosition position = new LogPosition(3, 4);
        final int txId = 42;

        // when
        cache.cacheTransactionMetadata(txId, position);
        cache.clear();
        final TransactionMetadataCache.TransactionMetadata metadata = cache.getTransactionMetadata(txId);

        // then
        assertNull(metadata);
    }

    @Test
    void shouldEvictOldValues() {
        var cache = new TransactionMetadataCache();
        for (int i = 0; i < 10_002; i++) {
            cache.cacheTransactionMetadata(i, new LogPosition(i, i));
        }

        assertThat(cache.getTransactionMetadata(0)).isNull();
        assertThat(cache.getTransactionMetadata(1)).isNull();

        for (int i = 2; i < 10_002; i++) {
            assertThat(cache.getTransactionMetadata(i))
                    .isEqualTo(new TransactionMetadataCache.TransactionMetadata(new LogPosition(i, i)));
        }
    }
}
