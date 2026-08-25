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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.neo4j.internal.kernel.api.EntityFilterBuilder;
import org.neo4j.internal.kernel.api.PreparedEntityFilter;
import org.neo4j.internal.kernel.api.SharedEntityFilterBuilder;
import org.neo4j.kernel.api.impl.index.SearcherReference;
import org.neo4j.memory.MemoryTracker;

/**
 * Resolves entity ids into ONE set of per-segment acceptDocs bitsets shared across all worker writers, for
 * the parallel runtime. Each writer resolves its ids in parallel (its own {@link EntityIdTermResolver}) but
 * sets the resolved bit into the shared bitset with a lock-free atomic OR. This keeps the (expensive)
 * seeking parallel while bounding the resolved-filter memory at O(index size) rather than
 * O(index size &times; worker count).
 */
public final class SharedLuceneEntityFilterBuilder implements SharedEntityFilterBuilder {

    // Atomic OR into a long[] element — lets many writer threads set bits in the same shared FixedBitSet
    // word without a lock. Only collisions on the SAME 64-bit word contend, which is rare when the filter
    // is sparse in the index (the large-index case this design targets).
    private static final VarHandle WORD = MethodHandles.arrayElementVarHandle(long[].class);

    private record Leaf(IndexReader.CacheKey coreCacheKey, FixedBitSet bits) {}

    private final String field;
    private final MemoryTracker memoryTracker;
    private final Leaf[] leaves;
    private final LeafReader[] readers;
    private final Bits[] liveDocs;
    private final ConcurrentLinkedQueue<Writer> writers = new ConcurrentLinkedQueue<>();
    private long bitsAllocated;
    private boolean built;

    public SharedLuceneEntityFilterBuilder(
            String field, List<SearcherReference> searchers, MemoryTracker memoryTracker) {
        this.field = field;
        this.memoryTracker = memoryTracker;
        List<Leaf> ls = new ArrayList<>();
        List<LeafReader> rs = new ArrayList<>();
        List<Bits> live = new ArrayList<>();
        for (SearcherReference ref : searchers) {
            for (LeafReaderContext ctx : ref.getIndexSearcher().leafContexts()) {
                LeafReader reader = ctx.reader();
                long bitsBytes = bitsetHeap(reader.maxDoc());
                memoryTracker.allocateHeap(bitsBytes);
                bitsAllocated += bitsBytes;
                ls.add(new Leaf(reader.getCoreCacheHelper().getKey(), new FixedBitSet(reader.maxDoc())));
                rs.add(reader);
                live.add(reader.getLiveDocs());
            }
        }
        this.leaves = ls.toArray(new Leaf[0]);
        this.readers = rs.toArray(new LeafReader[0]);
        this.liveDocs = live.toArray(new Bits[0]);
    }

    @Override
    public EntityFilterBuilder newWriter() {
        Writer w = new Writer();
        writers.add(w); // thread-safe: newWriter() is called concurrently by workers
        return w;
    }

    @Override
    public PreparedEntityFilter build() {
        if (built) {
            throw new IllegalStateException("build() already called");
        }
        // Flush each worker's final partial batch. This runs on the reducing thread (serial), but each tail
        // is < BATCH_SIZE seeks; the bulk of the seeking already happened in parallel in add().
        for (Writer w : writers) {
            w.flushAndClose();
        }
        Map<IndexReader.CacheKey, BitSet> bySegment = new IdentityHashMap<>(); // holds the dense FixedBitSets
        for (Leaf leaf : leaves) {
            bySegment.put(leaf.coreCacheKey(), leaf.bits());
        }
        PreparedEntityFilter filter = new LucenePreparedEntityFilter(bySegment, bitsAllocated, memoryTracker);
        bitsAllocated = 0; // ownership of the tracked bitset heap transfers to the filter
        built = true;
        return filter;
    }

    @Override
    public void close() {
        if (!built) {
            for (Writer w : writers) {
                w.close();
            }
            if (bitsAllocated > 0) {
                memoryTracker.releaseHeap(bitsAllocated);
                bitsAllocated = 0;
            }
        }
    }

    /** Set {@code doc}'s bit in leaf {@code leafOrd}'s shared bitset with an atomic OR (writers race). */
    private void orBit(int leafOrd, int doc) {
        long[] words = leaves[leafOrd].bits().getBits();
        WORD.getAndBitwiseOr(words, doc >>> 6, 1L << (doc & 63));
    }

    private static long bitsetHeap(int maxDoc) {
        return ((long) FixedBitSet.bits2words(maxDoc)) * Long.BYTES + 32;
    }

    /** Thread-confined per-worker writer: its own resolver (batch + enums), OR-ing into the shared bitsets. */
    private final class Writer implements EntityFilterBuilder {
        private final EntityIdTermResolver resolver = new EntityIdTermResolver(
                field, readers, liveDocs, SharedLuceneEntityFilterBuilder.this::orBit, memoryTracker);

        @Override
        public void add(long entityId) {
            resolver.add(entityId);
        }

        @Override
        public PreparedEntityFilter build() {
            throw new UnsupportedOperationException(
                    "writer is part of a SharedEntityFilterBuilder; call build() on the shared instance");
        }

        @Override
        public void close() {
            resolver.close();
        }

        void flushAndClose() {
            resolver.flush();
            resolver.close();
        }
    }
}
