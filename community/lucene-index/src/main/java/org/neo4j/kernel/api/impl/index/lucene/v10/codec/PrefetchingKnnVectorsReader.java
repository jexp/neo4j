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
import java.util.Map;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsFormat;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.util.Bits;
import org.neo4j.io.IOUtils;

/// Makes [KnnVectorValues#prefetch] reach the raw full-precision vectors in the `.vec` file.
///
/// Full-precision rescoring reads one raw vector per candidate, which on a large index is one page
/// fault each. Those faults can be queued up front instead of taken one at a time, but only if
/// something calls `prefetch`, and `prefetch` currently goes nowhere:
/// `Lucene104ScalarQuantizedVectorsReader.ScalarQuantizedVectorValues` forwards `vectorValue`,
/// `iterator`, `scorer` and `rescorer` to the raw vectors, yet not `prefetch`, so the values it
/// returns inherit the empty [KnnVectorValues#prefetch] body and silently do nothing.
///
/// A second [Lucene99FlatVectorsFormat] reader over the same segment recovers the raw values. The
/// quantized format writes its raw vectors through a reader-compatible writer of that same format,
/// using the same [SegmentReadState] and therefore the same segment suffix, so both readers see the
/// same `.vec` and `.vemf` files and hand out the same ordinals. The extra reader costs one
/// [org.apache.lucene.store.IndexInput] per field; mappings of one file share page cache, so it
/// duplicates a file handle rather than any memory.
///
/// Where it does reach the raw vectors, [RawVectorsPrefetch] issues the batch as merged ranges.
///
/// This whole class is a shim. Delete it once the quantized reader forwards `prefetch` itself.
class PrefetchingKnnVectorsReader extends KnnVectorsReader {
    private static final Lucene99FlatVectorsFormat RAW_VECTORS_FORMAT =
            new Lucene99FlatVectorsFormat(FlatVectorScorerUtil.getLucene99FlatVectorsScorer());

    private final KnnVectorsReader delegate;
    private final FlatVectorsReader rawVectorsReader;

    /// Takes ownership of `delegate`: on failure it is closed here, because nothing else can. The
    /// caller has it in a local, and [org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat]
    /// only closes readers it has already put in its map, so a second reader that fails to open
    /// would otherwise leak the segment's mappings on every attempt.
    static KnnVectorsReader wrap(KnnVectorsReader delegate, SegmentReadState state) throws IOException {
        boolean success = false;
        try {
            KnnVectorsReader reader = new PrefetchingKnnVectorsReader(delegate, RAW_VECTORS_FORMAT.fieldsReader(state));
            success = true;
            return reader;
        } finally {
            if (!success) {
                IOUtils.closeAllSilently(delegate);
            }
        }
    }

    private PrefetchingKnnVectorsReader(KnnVectorsReader delegate, FlatVectorsReader rawVectorsReader) {
        this.delegate = delegate;
        this.rawVectorsReader = rawVectorsReader;
    }

    @Override
    public FloatVectorValues getFloatVectorValues(String field) throws IOException {
        FloatVectorValues values = delegate.getFloatVectorValues(field);
        if (values == null) {
            return null;
        }
        FloatVectorValues rawValues = rawVectorsReader.getFloatVectorValues(field);
        // An empty raw view means the full-precision vectors were dropped from this segment and
        // rescoring dequantizes instead, so there is nothing in .vec to prefetch.
        if (rawValues == null || rawValues.size() == 0) {
            return values;
        }
        return new PrefetchingFloatVectorValues(values, rawValues);
    }

    @Override
    public ByteVectorValues getByteVectorValues(String field) throws IOException {
        return delegate.getByteVectorValues(field);
    }

    @Override
    public void search(String field, float[] target, KnnCollector knnCollector, AcceptDocs acceptDocs)
            throws IOException {
        delegate.search(field, target, knnCollector, acceptDocs);
    }

    @Override
    public void search(String field, byte[] target, KnnCollector knnCollector, AcceptDocs acceptDocs)
            throws IOException {
        delegate.search(field, target, knnCollector, acceptDocs);
    }

    @Override
    public void checkIntegrity() throws IOException {
        // the delegate holds its own reader over the same .vec and .vemf files, so checking it
        // covers everything this reader can see; checking twice would re-checksum the raw vectors
        delegate.checkIntegrity();
    }

    @Override
    public Map<String, Long> getOffHeapByteSize(FieldInfo fieldInfo) {
        // same reason as checkIntegrity: the extra reader maps the .vec the delegate already
        // reports, and mappings of one file share page cache, so adding it would double count
        return delegate.getOffHeapByteSize(fieldInfo);
    }

    @Override
    public KnnVectorsReader getMergeInstance() throws IOException {
        // merging never rescores, so it has nothing to prefetch
        return delegate.getMergeInstance();
    }

    @Override
    public void finishMerge() throws IOException {
        delegate.finishMerge();
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeAll(delegate, rawVectorsReader);
    }

    /// Delegates everything to the quantized values, except [#prefetch] which goes to the raw ones.
    ///
    /// Every method has to be forwarded explicitly. In particular [#scorer] must keep returning the
    /// quantized scorer used for graph traversal, and [#rescorer] the full-precision one -- letting
    /// either fall through to a default would quietly change what the query scores against.
    private static final class PrefetchingFloatVectorValues extends FloatVectorValues {
        private final FloatVectorValues delegate;
        private final FloatVectorValues rawValues;
        private final RawVectorsPrefetch prefetch;

        PrefetchingFloatVectorValues(FloatVectorValues delegate, FloatVectorValues rawValues) {
            this.delegate = delegate;
            this.rawValues = rawValues;
            this.prefetch = RawVectorsPrefetch.of(rawValues);
        }

        @Override
        public void prefetch(int[] ordsToPrefetch, int numOrds) throws IOException {
            if (prefetch != null) {
                prefetch.prefetch(ordsToPrefetch, numOrds);
            } else {
                rawValues.prefetch(ordsToPrefetch, numOrds);
            }
        }

        @Override
        public int dimension() {
            return delegate.dimension();
        }

        @Override
        public int size() {
            return delegate.size();
        }

        @Override
        public float[] vectorValue(int ord) throws IOException {
            return delegate.vectorValue(ord);
        }

        @Override
        public FloatVectorValues copy() throws IOException {
            return new PrefetchingFloatVectorValues(delegate.copy(), rawValues.copy());
        }

        @Override
        public int ordToDoc(int ord) {
            return delegate.ordToDoc(ord);
        }

        @Override
        public Bits getAcceptOrds(Bits acceptDocs) {
            return delegate.getAcceptOrds(acceptDocs);
        }

        @Override
        public DocIndexIterator iterator() {
            return delegate.iterator();
        }

        @Override
        public VectorScorer scorer(float[] target) throws IOException {
            return delegate.scorer(target);
        }

        @Override
        public VectorScorer rescorer(float[] target) throws IOException {
            return delegate.rescorer(target);
        }
    }
}
