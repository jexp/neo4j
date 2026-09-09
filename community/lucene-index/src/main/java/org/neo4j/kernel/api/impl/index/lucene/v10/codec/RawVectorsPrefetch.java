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
package org.neo4j.kernel.api.impl.index.lucene.v10.codec;

import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.codecs.lucene95.HasIndexSlice;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IntsRef;

/// Asks the kernel to start fetching a batch of full-precision vectors, as few `madvise` ranges as
/// the batch allows.
///
/// Rescoring reads one raw vector per candidate. On an index whose `.vec` does not fit in RAM each
/// of those is a major page fault, and issued one at a time from the query thread a query costs
/// `candidates * fault latency` while using a fraction of a core. The reads are independent, so the
/// only thing missing is queue depth, and `madvise(MADV_WILLNEED)` is how you buy it: it queues the
/// read and returns.
///
/// [org.apache.lucene.index.KnnVectorValues#prefetch] would do this already, except that the
/// quantized reader never forwards it -- [PrefetchingKnnVectorsReader] is what fixes that -- and
/// that [org.apache.lucene.codecs.lucene95.OffHeapFloatVectorValues] advises strictly one vector per
/// call, in whatever order the candidates arrived. This does three things it does not:
///
///   - sorts, so the ranges are advised in ascending file order;
///   - drops duplicates, which a candidate list can contain;
///   - merges consecutive ordinals into one range, so vectors sharing a page cost one call rather
///     than one each. At 768 dimensions a vector is 3072 bytes, so a pair of adjacent ordinals often
///     lands in a single page.
///
/// What it deliberately does not do is merge across gaps. Advising a range advises every page in it,
/// and reading pages nobody asked for is the read amplification this whole exercise is trying to
/// remove.
final class RawVectorsPrefetch {
    private final IndexInput slice;
    private final RawVectorAdvisor advisor;
    private final long vectorBytes;

    /// Reused across calls. One instance belongs to one [FloatVectorValues], which is already
    /// single-threaded by construction -- that is what `copy()` exists for.
    private int[] sorted = IntsRef.EMPTY_INTS;

    /// Null when the values cannot say where they read from, which is every non-mmap directory and
    /// so most tests. The caller falls back to Lucene's own per-ordinal path.
    static RawVectorsPrefetch of(FloatVectorValues rawValues) {
        return of(rawValues, RawVectorAdvisors.advisor());
    }

    /// Visible for testing: the advisor is the only part that needs a real memory mapping, so
    /// injecting it is what lets the coalescing be tested without one.
    static RawVectorsPrefetch of(FloatVectorValues rawValues, RawVectorAdvisor advisor) {
        if (!(rawValues instanceof HasIndexSlice hasSlice)) {
            return null;
        }
        IndexInput slice = hasSlice.getSlice();
        return slice == null ? null : new RawVectorsPrefetch(slice, advisor, rawValues.getVectorByteLength());
    }

    private RawVectorsPrefetch(IndexInput slice, RawVectorAdvisor advisor, long vectorBytes) {
        this.slice = slice;
        this.advisor = advisor;
        this.vectorBytes = vectorBytes;
    }

    void prefetch(int[] ords, int numOrds) throws IOException {
        int count = Math.min(numOrds, ords.length);
        if (count <= 1) {
            // Nothing to overlap: the rescoring read faults a lone vector in just as fast on its own.
            return;
        }

        // NoCopy: the previous batch is overwritten wholesale on the next line
        sorted = ArrayUtil.growNoCopy(sorted, count);
        System.arraycopy(ords, 0, sorted, 0, count);
        Arrays.sort(sorted, 0, count);

        long start = offsetOf(sorted[0]);
        long end = start + vectorBytes;
        for (int i = 1; i < count; i++) {
            long next = offsetOf(sorted[i]);
            if (next <= end) {
                // consecutive ordinal, or the same one twice
                end = Math.max(end, next + vectorBytes);
            } else {
                advise(start, end - start);
                start = next;
                end = next + vectorBytes;
            }
        }
        advise(start, end - start);
    }

    /// Lucene's own [IndexInput#prefetch] self-throttles to under 1% of the calls it is given -- see
    /// [RawVectorAdvisor] -- so go through the advisor where it can act, and fall back to Lucene
    /// only where it cannot: a non-mmap directory, a range spanning two mmap chunks, or a runtime
    /// without the syscall.
    private void advise(long offset, long length) throws IOException {
        if (!advisor.willNeed(slice, offset, length)) {
            slice.prefetch(offset, length);
        }
    }

    private long offsetOf(int ord) {
        return ord * vectorBytes;
    }
}
