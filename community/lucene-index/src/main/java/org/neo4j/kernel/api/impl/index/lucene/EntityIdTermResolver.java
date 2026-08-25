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
import java.io.UncheckedIOException;
import java.util.Arrays;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.neo4j.memory.MemoryTracker;

/**
 * Buffers entity ids and resolves each to the {@code (leaf, docId)} of its single live document via sorted
 * term {@code seekExact}s, invoking {@code onLiveHit} for every resolved doc. This is the term-seek/encode/
 * batch machinery shared by {@link LuceneEntityFilterBuilder} (single-threaded, sets bits into its own
 * per-segment bitsets) and {@link SharedLuceneEntityFilterBuilder} (per-worker, OR-s bits into a shared
 * bitset); the two differ only in what {@code onLiveHit} does with a resolved doc.
 * <p>
 * Working memory is bounded at {@code O(BATCH_SIZE)} — there is no {@code O(N)} id set. Each batch is
 * numeric-sorted before draining so the {@code seekExact} calls get term-dictionary block locality; the
 * sort is skipped when ids already arrived in ascending order (e.g. an id-ordered scan). The instance is
 * thread-confined: one per single-threaded builder, or one per worker in the shared builder.
 */
final class EntityIdTermResolver implements AutoCloseable {
    static final int BATCH_SIZE = 8 * 1024;

    /** Reports the {@code (leafOrd, doc)} of a resolved live document. */
    @FunctionalInterface
    interface LiveDocConsumer {
        void accept(int leafOrd, int doc) throws IOException;
    }

    private final String field;
    private final LeafReader[] readers;
    private final Bits[] liveDocs; // liveDocs[i] == null when leaf i has no deletions
    private final LiveDocConsumer onLiveHit;
    private final MemoryTracker memoryTracker;

    // Reused encode scratch for the term seek.
    private final BytesRefBuilder scratch = new BytesRefBuilder();
    private final byte[] digits = new byte[20]; // max digits of a long (19) + optional sign

    // Fixed buffer drained as it fills, plus per-leaf enums reused across drains.
    private final long[] batch = new long[BATCH_SIZE];
    private long batchAllocated;
    private int batchCount;
    private boolean batchSorted = true; // cleared when an id arrives out of ascending order
    private TermsEnum[] seekEnums;
    private PostingsEnum[] seekReuse;

    EntityIdTermResolver(
            String field,
            LeafReader[] readers,
            Bits[] liveDocs,
            LiveDocConsumer onLiveHit,
            MemoryTracker memoryTracker) {
        this.field = field;
        this.readers = readers;
        this.liveDocs = liveDocs;
        this.onLiveHit = onLiveHit;
        this.memoryTracker = memoryTracker;
        this.batchAllocated = (long) BATCH_SIZE * Long.BYTES + 16;
        memoryTracker.allocateHeap(batchAllocated);
    }

    /** Buffer an id, draining (resolving) the batch when it fills. */
    void add(long entityId) {
        if (batchCount > 0 && entityId < batch[batchCount - 1]) {
            batchSorted = false;
        }
        batch[batchCount++] = entityId;
        if (batchCount == BATCH_SIZE) {
            flush();
        }
    }

    /** Resolve the buffered ids: sort for block locality, then seek and report each live hit. */
    void flush() {
        if (batchCount == 0) {
            return;
        }
        try {
            // Arrays.sort(long[]) has no pre-sorted fast path, so we track monotonicity in add() and only
            // sort when needed. For ids of the same digit length numeric order coincides with term (decimal
            // byte) order, giving the seekExact calls block locality within the sorted batch.
            if (!batchSorted) {
                Arrays.sort(batch, 0, batchCount);
            }
            ensureSeekEnums();
            for (int k = 0; k < batchCount; k++) {
                seekAndReport(batch[k]);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        batchCount = 0;
        batchSorted = true;
    }

    @Override
    public void close() {
        if (batchAllocated > 0) {
            memoryTracker.releaseHeap(batchAllocated);
            batchAllocated = 0;
        }
    }

    /** Lazily open one reusable {@link TermsEnum}/{@link PostingsEnum} per leaf. */
    private void ensureSeekEnums() throws IOException {
        if (seekEnums == null) {
            seekEnums = new TermsEnum[readers.length];
            seekReuse = new PostingsEnum[readers.length];
            for (int i = 0; i < readers.length; i++) {
                Terms terms = readers[i].terms(field);
                seekEnums[i] = terms == null ? null : terms.iterator();
            }
        }
    }

    private void seekAndReport(long id) throws IOException {
        // The index stores the id as its decimal string (see Lucene10ReusableDocWithId). Write the ASCII
        // digits straight into the reused buffer instead of Long.toString + a UTF-16 -> UTF-8 conversion.
        writeDecimal(scratch, digits, id);
        BytesRef term = scratch.get();
        for (int i = 0; i < seekEnums.length; i++) {
            TermsEnum te = seekEnums[i];
            if (te == null || !te.seekExact(term)) {
                continue;
            }
            // A term hit may be a stale (deleted) copy whose live document lives in a later leaf -- an
            // entity update is delete-by-term + add -- so skip dead docs and stop at the first live one.
            // Lucene ands acceptDocs with liveDocs, so reporting a dead doc would silently drop the entity.
            seekReuse[i] = te.postings(seekReuse[i], PostingsEnum.NONE);
            int doc;
            while ((doc = seekReuse[i].nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                if (isLive(i, doc)) {
                    onLiveHit.accept(i, doc); // id is unique among live docs -> resolves in exactly one leaf
                    return;
                }
            }
        }
    }

    private boolean isLive(int leaf, int doc) {
        Bits live = liveDocs[leaf];
        return live == null || live.get(doc);
    }

    /**
     * Encode {@code value} as decimal ASCII into {@code scratch}, producing the exact bytes of
     * {@code Long.toString(value)} without allocating a String or doing a UTF-16 -> UTF-8 conversion.
     * {@code digits} is a reusable scratch array of length >= 20.
     */
    static void writeDecimal(BytesRefBuilder scratch, byte[] digits, long value) {
        int pos = digits.length;
        long v = value;
        if (value >= 0) {
            do {
                digits[--pos] = (byte) ('0' + (int) (v % 10));
                v /= 10;
            } while (v != 0);
        } else {
            // v % 10 is <= 0 here, so '0' - (v % 10) yields the digit; never negates (Long.MIN_VALUE safe).
            do {
                digits[--pos] = (byte) ('0' - (int) (v % 10));
                v /= 10;
            } while (v != 0);
            digits[--pos] = '-';
        }
        scratch.copyBytes(digits, pos, digits.length - pos);
    }
}
