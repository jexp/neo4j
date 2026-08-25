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

import java.io.IOException;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.SparseFixedBitSet;
import org.neo4j.internal.kernel.api.EntityFilterBuilder;
import org.neo4j.internal.kernel.api.PreparedEntityFilter;
import org.neo4j.kernel.api.impl.index.SearcherReference;
import org.neo4j.memory.MemoryTracker;

/**
 * Resolves entity ids into per-segment acceptDocs bitsets bound to a single index snapshot. The term
 * seeking is done by an {@link EntityIdTermResolver}; this class owns the per-segment bitsets each resolved
 * doc is set into.
 */
public final class LuceneEntityFilterBuilder implements EntityFilterBuilder {

    // Per-segment acceptDocs. Starts as a SparseFixedBitSet (O(#matches)) and self-promotes to a dense
    // FixedBitSet once the populated fraction crosses ~1/128 of the segment (same threshold as Lucene's
    // BitSet#of). So selective/clustered filters stay O(#matches), while a filter that turns out dense
    // avoids SparseFixedBitSet's per-block blow-up and slower get().
    private static final class LeafState {
        final IndexReader.CacheKey coreCacheKey;
        BitSet bits;
        private final int maxDoc;
        private final int denseThreshold;
        private int setCount;
        private boolean dense;

        LeafState(IndexReader.CacheKey coreCacheKey, int maxDoc) {
            this.coreCacheKey = coreCacheKey;
            this.maxDoc = maxDoc;
            this.denseThreshold = maxDoc >>> 7; // ~0.78%, same as BitSet#of
            this.bits = new SparseFixedBitSet(maxDoc);
        }

        void set(int doc) throws IOException {
            bits.set(doc);
            if (!dense && ++setCount > denseThreshold) {
                FixedBitSet promoted = new FixedBitSet(maxDoc);
                promoted.or(new BitSetIterator(bits, setCount)); // drain the sparse bits into the dense one
                bits = promoted;
                dense = true;
            }
        }
    }

    private final MemoryTracker memoryTracker;
    private final LeafState[] leaves;
    private final EntityIdTermResolver resolver;
    private long bitsAllocated;
    private boolean built;

    public LuceneEntityFilterBuilder(String field, List<SearcherReference> searchers, MemoryTracker memoryTracker) {
        this.memoryTracker = memoryTracker;
        List<LeafState> ls = new ArrayList<>();
        List<LeafReader> readers = new ArrayList<>();
        List<Bits> liveDocs = new ArrayList<>();
        for (SearcherReference ref : searchers) {
            for (LeafReaderContext ctx : ref.getIndexSearcher().leafContexts()) {
                LeafReader reader = ctx.reader();
                // Heap is reserved at the dense bound up front, which over-reserves while a segment stays sparse.
                long bitsBytes = bitsetHeap(reader.maxDoc());
                memoryTracker.allocateHeap(bitsBytes);
                bitsAllocated += bitsBytes;
                ls.add(new LeafState(reader.getCoreCacheHelper().getKey(), reader.maxDoc()));
                readers.add(reader);
                liveDocs.add(reader.getLiveDocs());
            }
        }
        this.leaves = ls.toArray(new LeafState[0]);
        this.resolver = new EntityIdTermResolver(
                field,
                readers.toArray(new LeafReader[0]),
                liveDocs.toArray(new Bits[0]),
                (leafOrd, doc) -> leaves[leafOrd].set(doc), // sets the bit, promoting sparse->dense if dense
                memoryTracker);
    }

    @Override
    public void add(long entityId) {
        resolver.add(entityId);
    }

    @Override
    public PreparedEntityFilter build() {
        resolver.flush(); // resolve the final partial batch
        Map<IndexReader.CacheKey, BitSet> bySegment = new IdentityHashMap<>();
        for (LeafState leaf : leaves) {
            bySegment.put(leaf.coreCacheKey, leaf.bits);
        }
        LucenePreparedEntityFilter filter = new LucenePreparedEntityFilter(bySegment, bitsAllocated, memoryTracker);
        bitsAllocated = 0; // ownership of the tracked bitset heap transfers to the filter
        built = true;
        resolver.close(); // release the drain buffer
        return filter;
    }

    @Override
    public void close() {
        if (!built) {
            if (bitsAllocated > 0) {
                memoryTracker.releaseHeap(bitsAllocated);
                bitsAllocated = 0;
            }
            resolver.close();
        }
    }

    private static long bitsetHeap(int maxDoc) {
        // long[] words + FixedBitSet object header, approximate
        return ((long) FixedBitSet.bits2words(maxDoc)) * Long.BYTES + 32;
    }
}
