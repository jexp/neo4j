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
package org.neo4j.kernel.api.impl.index.lucene;

import java.util.Map;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.neo4j.internal.kernel.api.PreparedEntityFilter;
import org.neo4j.memory.MemoryTracker;

/**
 * Lucene-backed {@link PreparedEntityFilter}. Holds the already-resolved acceptDocs bitset per
 * segment, keyed by the segment's {@code CoreCacheHelper} key so that a single filter query can be
 * executed across every partition of the vector index (the same query object is reused per
 * partition, see {@code VectorIndexReader#indexProgressor}).
 */
public final class LucenePreparedEntityFilter implements PreparedEntityFilter {
    // keyed by LeafReader core cache key; BitSet so a segment can be Sparse (O(matches)) or Fixed (dense)
    private final Map<IndexReader.CacheKey, BitSet> bySegment;
    private final long heapEstimate;
    private final MemoryTracker memoryTracker;
    private boolean closed;

    LucenePreparedEntityFilter(
            Map<IndexReader.CacheKey, BitSet> bySegment, long heapEstimate, MemoryTracker memoryTracker) {
        this.bySegment = bySegment;
        this.heapEstimate = heapEstimate;
        this.memoryTracker = memoryTracker;
    }

    /** Prebuilt acceptDocs for the given segment, or {@code null} if none of its docs matched. */
    public BitSet bitSetFor(IndexReader.CacheKey coreCacheKey) {
        return bySegment.get(coreCacheKey);
    }

    @Override
    public long heapEstimate() {
        return heapEstimate;
    }

    @Override
    public void combine(PreparedEntityFilter other) {
        // Same snapshot => identical segment key sets, so this is a pure in-place OR with no new allocation.
        LucenePreparedEntityFilter o = (LucenePreparedEntityFilter) other;
        try {
            for (Map.Entry<IndexReader.CacheKey, BitSet> entry : o.bySegment.entrySet()) {
                BitSet mine = bySegment.get(entry.getKey());
                if (mine != null) {
                    // BitSet.or takes an iterator (works across Fixed/Sparse mixes), not another BitSet.
                    // In-memory iteration, so the declared IOException never actually fires.
                    BitSet theirs = entry.getValue();
                    mine.or(new BitSetIterator(theirs, theirs.cardinality()));
                } else {
                    bySegment.put(entry.getKey(), entry.getValue());
                }
            }
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        bySegment.clear();
        if (heapEstimate > 0) {
            memoryTracker.releaseHeap(heapEstimate);
        }
    }
}
