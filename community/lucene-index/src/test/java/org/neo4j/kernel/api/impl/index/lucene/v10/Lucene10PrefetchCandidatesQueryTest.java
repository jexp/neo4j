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
package org.neo4j.kernel.api.impl.index.lucene.v10;

import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.kernel.api.impl.index.lucene.LuceneIndexWriterConfig.MergePolicyOption.LOG_BYTE_SIZED;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SettingsAccessor;
import org.neo4j.internal.schema.SettingsAccessor.IndexSettingObjectMapAccessor;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.api.impl.index.collector.ValuesIterator;
import org.neo4j.kernel.api.impl.index.lucene.LuceneContext;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDirectory;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDirectoryReader;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDocument;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDocumentsFactory;
import org.neo4j.kernel.api.impl.index.lucene.LuceneIndexSearcher;
import org.neo4j.kernel.api.impl.index.lucene.LuceneIndexWriter;
import org.neo4j.kernel.api.impl.index.lucene.LuceneIndexWriterConfig;
import org.neo4j.kernel.api.impl.index.lucene.LuceneQueryContext;
import org.neo4j.kernel.api.impl.index.lucene.LuceneSettings;
import org.neo4j.kernel.api.impl.index.lucene.codec.LuceneCodec;
import org.neo4j.kernel.api.impl.index.lucene.v10.Lucene10QueryContext.PrefetchCandidatesQuery.PrefetchingDocIdSetIterator;
import org.neo4j.kernel.api.impl.schema.LuceneQueryFactory.VectorQueryFactory;
import org.neo4j.kernel.api.impl.schema.vector.Neo4jVectorSimilarityFunction;
import org.neo4j.kernel.api.impl.schema.vector.VectorDocumentStructure;
import org.neo4j.kernel.api.impl.schema.vector.VectorDocumentStructureAccess;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfig;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexVersion;
import org.neo4j.kernel.api.impl.schema.vector.VectorQuantizationType;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

class Lucene10PrefetchCandidatesQueryTest {
    private static final LuceneContext CONTEXT = LuceneContext.LUCENE_10;
    private static final KernelVersion KERNEL_VERSION = KernelVersion.getLatestVersion(Config.defaults());
    private static final VectorIndexVersion VECTOR_INDEX_VERSION =
            VectorIndexVersion.latestSupportedVersion(KERNEL_VERSION);
    private static final int MAX_EF_SEARCH = Config.defaults().get(LuceneSettings.vector_hnsw_max_ef_search);

    private static final int DIMENSIONS = 64;
    private static final int DOCUMENTS_PER_SEGMENT = 15;
    private static final int SEGMENTS = 2;
    private static final int DOCUMENTS = DOCUMENTS_PER_SEGMENT * SEGMENTS;

    private static final int TOP_K = 5;
    // large enough that every document becomes a rescore candidate, so the rescored order is
    // decided entirely by full precision and the expected answer is exact rather than approximate
    private static final double SEARCH_EXPANSION = 20.0;

    /// Documents differ from each other in a single dimension, so their one-bit codes are nearly
    /// identical and the graph cannot order them. Only full-precision rescoring can, which is what
    /// makes the expected ranking a real assertion about the rescore path.
    private static float[] embedding(int id) {
        float[] embedding = new float[DIMENSIONS];
        java.util.Arrays.fill(embedding, 0.5f);
        embedding[0] = 0.5f + id * 0.1f;
        return embedding;
    }

    private static float[] queryEmbedding() {
        float[] embedding = new float[DIMENSIONS];
        java.util.Arrays.fill(embedding, 0.5f);
        return embedding;
    }

    @Test
    void shouldReturnTheTrueNearestNeighboursWhenRescoring() throws Exception {
        try (LuceneDirectory directory = binaryQuantizedIndex()) {
            try (LuceneDirectoryReader indexReader = directory.open();
                    LuceneIndexSearcher indexSearcher = indexReader.newDirectSearcher()) {

                IndexQueryConstraints constraints =
                        IndexQueryConstraints.unconstrained().limit(TOP_K);
                LuceneQueryContext queryContext = new VectorQueryFactory(
                                documentStructure(), VectorQuantizationType.BINARY, SEARCH_EXPANSION, MAX_EF_SEARCH)
                        .createQuery(
                                indexSearcher,
                                constraints,
                                IndexDescriptor.NO_INDEX,
                                PropertyIndexQuery.nearestNeighbors(TOP_K, SEARCH_EXPANSION, queryEmbedding()));

                List<Long> actual = new ArrayList<>();
                ValuesIterator results = indexSearcher.searchVectors(queryContext, constraints);
                while (results.hasNext()) {
                    actual.add(results.next());
                }

                // embedding(id) sits at distance id * 0.1 from the query, so ids 0..TOP_K-1 are the
                // true nearest and there are no ties to make the ordering ambiguous
                assertThat(actual).containsExactly(0L, 1L, 2L, 3L, 4L);
            }
        }
    }

    @Test
    void shouldInstallTheReaderThatForwardsPrefetchToTheRawVectors() throws Exception {
        try (LuceneDirectory directory = binaryQuantizedIndex()) {
            try (LuceneDirectoryReader indexReader = directory.open()) {
                for (LeafReaderContext leaf : leaves(indexReader)) {
                    FloatVectorValues values = leaf.reader().getFloatVectorValues(fieldName());
                    assertThat(values).isNotNull();
                    assertThat(values.getClass().getSimpleName())
                            .as("prefetch must not fall through to the no-op in KnnVectorValues")
                            .isEqualTo("PrefetchingFloatVectorValues");

                    // reaches OffHeapFloatVectorValues.prefetch, which needs at least two ordinals
                    values.prefetch(new int[] {0, 1}, 2);
                }
            }
        }
    }

    @Test
    void shouldYieldExactlyTheCandidatesOfTheInnerQuery() throws Exception {
        int[] candidates = {0, 3, 4, 9, 14};

        try (LuceneDirectory directory = binaryQuantizedIndex()) {
            try (LuceneDirectoryReader indexReader = directory.open()) {
                LeafReaderContext leaf = leaves(indexReader).get(0);
                PrefetchingDocIdSetIterator iterator =
                        new PrefetchingDocIdSetIterator(iteratorOver(candidates), leaf, fieldName());

                assertThat(iterator.docID()).isEqualTo(-1);
                for (int candidate : candidates) {
                    assertThat(iterator.nextDoc()).isEqualTo(candidate);
                    assertThat(iterator.docID()).isEqualTo(candidate);
                }
                assertThat(iterator.nextDoc()).isEqualTo(DocIdSetIterator.NO_MORE_DOCS);
                assertThat(iterator.docID()).isEqualTo(DocIdSetIterator.NO_MORE_DOCS);
            }
        }
    }

    @Test
    void shouldAdvanceToTheFirstCandidateAtOrAfterTheTarget() throws Exception {
        int[] candidates = {0, 3, 4, 9, 14};

        try (LuceneDirectory directory = binaryQuantizedIndex()) {
            try (LuceneDirectoryReader indexReader = directory.open()) {
                LeafReaderContext leaf = leaves(indexReader).get(0);
                String field = fieldName();

                assertThat(new PrefetchingDocIdSetIterator(iteratorOver(candidates), leaf, field).advance(1))
                        .as("skips to the next candidate when the target is not one")
                        .isEqualTo(3);
                assertThat(new PrefetchingDocIdSetIterator(iteratorOver(candidates), leaf, field).advance(4))
                        .as("lands on the target when it is a candidate")
                        .isEqualTo(4);
                assertThat(new PrefetchingDocIdSetIterator(iteratorOver(candidates), leaf, field).advance(15))
                        .as("exhausts past the last candidate")
                        .isEqualTo(DocIdSetIterator.NO_MORE_DOCS);

                PrefetchingDocIdSetIterator iterator =
                        new PrefetchingDocIdSetIterator(iteratorOver(candidates), leaf, field);
                assertThat(iterator.nextDoc()).isEqualTo(0);
                assertThat(iterator.advance(4)).isEqualTo(4);
                assertThat(iterator.nextDoc()).isEqualTo(9);
            }
        }
    }

    @Test
    void shouldHandleASingleCandidate() throws Exception {
        try (LuceneDirectory directory = binaryQuantizedIndex()) {
            try (LuceneDirectoryReader indexReader = directory.open()) {
                LeafReaderContext leaf = leaves(indexReader).get(0);
                // below two ordinals the prefetch is skipped, but iteration must still work
                PrefetchingDocIdSetIterator iterator =
                        new PrefetchingDocIdSetIterator(iteratorOver(new int[] {7}), leaf, fieldName());

                assertThat(iterator.nextDoc()).isEqualTo(7);
                assertThat(iterator.nextDoc()).isEqualTo(DocIdSetIterator.NO_MORE_DOCS);
            }
        }
    }

    @Test
    void shouldHandleNoCandidates() throws Exception {
        try (LuceneDirectory directory = binaryQuantizedIndex()) {
            try (LuceneDirectoryReader indexReader = directory.open()) {
                LeafReaderContext leaf = leaves(indexReader).get(0);
                PrefetchingDocIdSetIterator iterator =
                        new PrefetchingDocIdSetIterator(iteratorOver(new int[0]), leaf, fieldName());

                assertThat(iterator.nextDoc()).isEqualTo(DocIdSetIterator.NO_MORE_DOCS);
                assertThat(iterator.advance(0)).isEqualTo(DocIdSetIterator.NO_MORE_DOCS);
            }
        }
    }

    @Test
    void shouldPrefetchEveryCandidateOrdinal() throws Exception {
        List<Integer> prefetched = new ArrayList<>();
        int[] candidates = {0, 3, 4, 9, 14};

        try (LuceneDirectory directory = binaryQuantizedIndex()) {
            try (LuceneDirectoryReader indexReader = directory.open()) {
                LeafReaderContext leaf = recordingPrefetches(leaves(indexReader).get(0), prefetched);
                PrefetchingDocIdSetIterator iterator =
                        new PrefetchingDocIdSetIterator(iteratorOver(candidates), leaf, fieldName());

                // prefetching happens when the candidate set is drained, on the first advance
                iterator.nextDoc();

                // this segment is dense, so ordinal == docId
                assertThat(prefetched).containsExactly(0, 3, 4, 9, 14);
            }
        }
    }

    private static DocIdSetIterator iteratorOver(int[] docs) {
        return new DocIdSetIterator() {
            private int index = -1;

            @Override
            public int docID() {
                return index < 0 ? -1 : (index < docs.length ? docs[index] : NO_MORE_DOCS);
            }

            @Override
            public int nextDoc() {
                index++;
                return docID();
            }

            @Override
            public int advance(int target) {
                while (nextDoc() < target) {
                    // keep going
                }
                return docID();
            }

            @Override
            public long cost() {
                return docs.length;
            }
        };
    }

    /// Wraps the leaf so the vector values record which ordinals were handed to `prefetch`.
    private static LeafReaderContext recordingPrefetches(LeafReaderContext leaf, List<Integer> prefetched)
            throws IOException {
        org.apache.lucene.index.FilterLeafReader reader = new org.apache.lucene.index.FilterLeafReader(leaf.reader()) {
            @Override
            public FloatVectorValues getFloatVectorValues(String field) throws IOException {
                FloatVectorValues delegate = super.getFloatVectorValues(field);
                return delegate == null ? null : new RecordingFloatVectorValues(delegate, prefetched);
            }

            @Override
            public org.apache.lucene.index.IndexReader.CacheHelper getCoreCacheHelper() {
                return in.getCoreCacheHelper();
            }

            @Override
            public org.apache.lucene.index.IndexReader.CacheHelper getReaderCacheHelper() {
                return in.getReaderCacheHelper();
            }
        };
        return reader.getContext();
    }

    private static final class RecordingFloatVectorValues extends FloatVectorValues {
        private final FloatVectorValues delegate;
        private final List<Integer> prefetched;

        RecordingFloatVectorValues(FloatVectorValues delegate, List<Integer> prefetched) {
            this.delegate = delegate;
            this.prefetched = prefetched;
        }

        @Override
        public void prefetch(int[] ordsToPrefetch, int numOrds) throws IOException {
            for (int i = 0; i < numOrds; i++) {
                prefetched.add(ordsToPrefetch[i]);
            }
            delegate.prefetch(ordsToPrefetch, numOrds);
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
            return new RecordingFloatVectorValues(delegate.copy(), prefetched);
        }

        @Override
        public DocIndexIterator iterator() {
            return delegate.iterator();
        }

        @Override
        public int ordToDoc(int ord) {
            return delegate.ordToDoc(ord);
        }
    }

    private static List<LeafReaderContext> leaves(LuceneDirectoryReader indexReader) {
        return ((Lucene10DirectoryReader) indexReader).reader.leaves();
    }

    private static VectorIndexConfig indexConfig() {
        SettingsAccessor indexSettings = new IndexSettingObjectMapAccessor(Map.ofEntries(
                entry(IndexSetting.vector_Dimensions(), DIMENSIONS),
                entry(IndexSetting.vector_Similarity_Function(), "EUCLIDEAN"),
                entry(IndexSetting.vector_Quantization_Type(), VectorQuantizationType.BINARY.name())));
        return VECTOR_INDEX_VERSION.indexSettingValidator(KERNEL_VERSION).validateToTypedConfig(indexSettings);
    }

    private static VectorDocumentStructure documentStructure() {
        return VectorDocumentStructureAccess.documentStructureFor(VECTOR_INDEX_VERSION);
    }

    private static String fieldName() {
        return documentStructure().vectorValueKeyFor(DIMENSIONS);
    }

    private static LuceneDirectory binaryQuantizedIndex() throws IOException {
        VectorIndexConfig config = indexConfig();
        LuceneCodec codec = CONTEXT.codecsFactory().codecFor(config);
        LuceneIndexWriterConfig writerConfig = new LuceneIndexWriterConfig(new KeywordAnalyzer()).setCodec(codec);
        // never merge, so the two commits stay two segments and the per-leaf path is exercised
        writerConfig.setMergingParameters(1.0, 10, LOG_BYTE_SIZED, 10, 10, 1000, 8.0, 1000);

        VectorDocumentStructure documentStructure = documentStructure();
        LuceneDocumentsFactory documentFactory = CONTEXT.documentsFactory();
        Neo4jVectorSimilarityFunction similarityFunction = (Neo4jVectorSimilarityFunction) config.similarityFunction();

        LuceneDirectory directory = CONTEXT.directoryFactory().inMemoryDirectory();
        try (LuceneIndexWriter indexWriter = directory.newWriter(writerConfig)) {
            for (int segment = 0; segment < SEGMENTS; segment++) {
                for (int i = 0; i < DOCUMENTS_PER_SEGMENT; i++) {
                    long id = (long) segment * DOCUMENTS_PER_SEGMENT + i;
                    Value[] values = new Value[] {Values.floatArray(embedding((int) id))};
                    LuceneDocument document =
                            documentFactory.createVectorDocument(documentStructure, id, similarityFunction, values);
                    indexWriter.addDocument(document);
                }
                indexWriter.commit();
            }
        }
        return directory;
    }

    @Test
    void sanityCheckTheExpectedRanking() {
        // guards the fixture itself: the nearest TOP_K by true euclidean distance must be ids 0..4
        float[] query = queryEmbedding();
        List<Integer> nearest = new ArrayList<>();
        for (int id = 0; id < DOCUMENTS; id++) {
            nearest.add(id);
        }
        nearest.sort(Comparator.comparingDouble(id -> squaredDistance(query, embedding(id))));
        assertThat(nearest.subList(0, TOP_K)).containsExactly(0, 1, 2, 3, 4);
    }

    private static double squaredDistance(float[] a, float[] b) {
        double sum = 0.0;
        for (int i = 0; i < a.length; i++) {
            double diff = a[i] - b[i];
            sum += diff * diff;
        }
        return sum;
    }
}
