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

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.kernel.api.impl.index.lucene.LuceneDocumentsFactory.ENTITY_ID_KEY;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.SparseFixedBitSet;
import org.junit.jupiter.api.Test;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.internal.kernel.api.PreparedEntityFilter;
import org.neo4j.kernel.api.impl.index.SearcherReference;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.memory.LocalMemoryTracker;

class LuceneEntityFilterBuilderTest {
    private static final LuceneIndexWriterConfig WRITER_CONFIG =
            LuceneIndexWriterConfig.analyzerOnly(new KeywordAnalyzer());

    @Test
    void resolvesIdsToDocIds() throws Exception {
        // entity id i is written as doc i (single segment, insertion order)
        try (Index index = buildIndex(ids(0, 10))) {
            LucenePreparedEntityFilter filter = build(index, 2, 5, 7);
            assertThat(setBits(filter.bitSetFor(index.segmentKey(0)))).containsExactly(2, 5, 7);
        }
    }

    @Test
    void ignoresIdsNotInTheIndex() throws Exception {
        try (Index index = buildIndex(ids(0, 10))) {
            LucenePreparedEntityFilter filter = build(index, 2, 999, 7);
            assertThat(setBits(filter.bitSetFor(index.segmentKey(0)))).containsExactly(2, 7);
        }
    }

    @Test
    void emptyFilterSetsNoBits() throws Exception {
        try (Index index = buildIndex(ids(0, 10))) {
            LucenePreparedEntityFilter filter = build(index);
            assertThat(filter.bitSetFor(index.segmentKey(0)).cardinality()).isZero();
        }
    }

    @Test
    void resolvesUnsortedInput() throws Exception {
        // ids added out of ascending order must still resolve correctly (drainBatch sorts them)
        try (Index index = buildIndex(ids(0, 10))) {
            LucenePreparedEntityFilter filter = build(index, 7, 2, 9, 0, 5);
            assertThat(setBits(filter.bitSetFor(index.segmentKey(0)))).containsExactly(0, 2, 5, 7, 9);
        }
    }

    @Test
    void resolvesAcrossSegments() throws Exception {
        // two segments: ids [0..4] in segment 0, ids [5..9] in segment 1 (each starts at docId 0)
        try (Index index = buildIndex(ids(0, 5), ids(5, 10))) {
            LucenePreparedEntityFilter filter = build(index, 2, 7);
            assertThat(setBits(filter.bitSetFor(index.segmentKey(0)))).containsExactly(2); // id 2 -> seg0 doc2
            assertThat(setBits(filter.bitSetFor(index.segmentKey(1)))).containsExactly(2); // id 7 -> seg1 doc2
        }
    }

    @Test
    void combineUnionsBitsets() throws Exception {
        try (Index index = buildIndex(ids(0, 10))) {
            PreparedEntityFilter a = build(index, 2, 3);
            PreparedEntityFilter b = build(index, 5, 8);
            a.combine(b);
            b.close();
            assertThat(setBits(((LucenePreparedEntityFilter) a).bitSetFor(index.segmentKey(0))))
                    .containsExactly(2, 3, 5, 8);
        }
    }

    @Test
    void resolvesLiveDocWhenDeletedCopyLivesInAnEarlierSegment() throws Exception {
        // An entity update is delete-by-term + add, so after updating id 5 its term exists in two segments:
        // dead in the first, live in the second. The dead copy must not shadow the live one.
        try (Index index = buildIndex(writer -> {
            for (long id = 0; id < 10; id++) {
                addDocument(writer, id);
            }
            writer.commit();
            updateDocument(writer, 5);
            writer.commit();
        })) {
            assertThat(index.searcher().leafContexts())
                    .as("no merge collapsed the delete away")
                    .hasSize(2);

            List<Accepted> accepted = accepted(index, build(index, 5));

            assertThat(accepted)
                    .as("the live document for entity 5 is accepted")
                    .extracting(Accepted::id)
                    .containsExactly("5");
            assertThat(accepted)
                    .as("no deleted document is accepted -- Lucene ands acceptDocs with liveDocs, so a bit "
                            + "set on a dead doc silently drops the entity from the filter")
                    .allMatch(Accepted::live);
        }
    }

    @Test
    void resolvesLiveDocWhenDeletedCopyPrecedesItInTheSameSegment() throws Exception {
        // Two writes of id 5 without an intervening commit: the delete-by-term also applies to the buffered
        // document, so one segment ends up holding the dead copy at a *lower* docId than the live one and the
        // term's postings are {dead, live}. Taking the first posting would resolve to the dead doc.
        try (Index index = buildIndex(writer -> {
            for (long id = 0; id < 10; id++) {
                addDocument(writer, id);
            }
            updateDocument(writer, 5);
            writer.commit();
        })) {
            assertThat(index.searcher().leafContexts())
                    .as("single segment holding both the dead and the live copy")
                    .hasSize(1);

            List<Accepted> accepted = accepted(index, build(index, 5));

            assertThat(accepted).extracting(Accepted::id).containsExactly("5");
            assertThat(accepted).allMatch(Accepted::live);
            assertThat(accepted).extracting(Accepted::doc).containsExactly(10);
        }
    }

    @Test
    void largeInputTriggersBatchDrain() throws Exception {
        try (Index index = buildIndex(ids(0, 10))) {
            LuceneEntityFilterBuilder builder =
                    new LuceneEntityFilterBuilder(ENTITY_ID_KEY, List.of(index.asRef()), INSTANCE);
            for (long id = 0; id < 200_000; id++) {
                builder.add(id);
            }
            LucenePreparedEntityFilter filter = (LucenePreparedEntityFilter) builder.build();
            assertThat(setBits(filter.bitSetFor(index.segmentKey(0)))).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        }
    }

    @Test
    void tracksHeapAndReleasesOnFilterClose() throws Exception {
        try (Index index = buildIndex(ids(0, 10))) {
            LocalMemoryTracker tracker = new LocalMemoryTracker();
            LuceneEntityFilterBuilder builder =
                    new LuceneEntityFilterBuilder(ENTITY_ID_KEY, List.of(index.asRef()), tracker);
            assertThat(tracker.estimatedHeapMemory())
                    .as("buffer + bitsets tracked")
                    .isGreaterThan(0);

            builder.add(1);
            PreparedEntityFilter filter = builder.build();
            // build() releases the drain buffer and transfers bitset ownership to the filter
            assertThat(tracker.estimatedHeapMemory()).isEqualTo(filter.heapEstimate());

            filter.close();
            assertThat(tracker.estimatedHeapMemory())
                    .as("all released after filter close")
                    .isZero();
        }
    }

    @Test
    void releasesHeapWhenClosedWithoutBuild() throws Exception {
        try (Index index = buildIndex(ids(0, 10))) {
            LocalMemoryTracker tracker = new LocalMemoryTracker();
            LuceneEntityFilterBuilder builder =
                    new LuceneEntityFilterBuilder(ENTITY_ID_KEY, List.of(index.asRef()), tracker);
            builder.add(1);
            builder.close();
            assertThat(tracker.estimatedHeapMemory()).isZero();
        }
    }

    // ---- sparse <-> dense acceptDocs (adaptive promotion at maxDoc>>>7) ----------------------------

    @Test
    void staysSparseForSelectiveFilter() throws Exception {
        // maxDoc 5000 => promotion threshold maxDoc>>>7 = 39; 20 matches stays well under it
        try (Index index = buildIndex(ids(0, 5000))) {
            assertThat(index.searcher().leafContexts())
                    .as("test assumes a single segment; force-merge if Lucene split it")
                    .hasSize(1);
            BitSet bits = buildRange(index, 20).bitSetFor(index.segmentKey(0));
            assertThat(bits).isInstanceOf(SparseFixedBitSet.class);
            assertThat(setBits(bits))
                    .containsExactly(java.util.stream.IntStream.range(0, 20).toArray());
        }
    }

    @Test
    void promotesToDenseWhenSegmentGetsDense() throws Exception {
        // 100 matches exceeds the 39 threshold, so the segment flips sparse -> dense mid-build;
        // promotion must preserve every bit set both before and after the flip.
        try (Index index = buildIndex(ids(0, 5000))) {
            assertThat(index.searcher().leafContexts()).hasSize(1);
            BitSet bits = buildRange(index, 100).bitSetFor(index.segmentKey(0));
            assertThat(bits).isInstanceOf(FixedBitSet.class);
            assertThat(setBits(bits))
                    .containsExactly(java.util.stream.IntStream.range(0, 100).toArray());
        }
    }

    @Test
    void promotesJustAboveThreshold() throws Exception {
        try (Index index = buildIndex(ids(0, 5000))) { // threshold 39
            assertThat(index.searcher().leafContexts()).hasSize(1);
            assertThat(buildRange(index, 39).bitSetFor(index.segmentKey(0)))
                    .as("at the threshold => still sparse")
                    .isInstanceOf(SparseFixedBitSet.class);
            assertThat(buildRange(index, 40).bitSetFor(index.segmentKey(0)))
                    .as("one past the threshold => promoted to dense")
                    .isInstanceOf(FixedBitSet.class);
        }
    }

    @Test
    void combineUnionsSparseAndDenseBitsets() throws Exception {
        // a: 20 matches (sparse); b: 100 matches (promoted to dense) -> combine must union across types
        try (Index index = buildIndex(ids(0, 5000))) {
            assertThat(index.searcher().leafContexts()).hasSize(1);
            PreparedEntityFilter a = buildRange(index, 20); // SparseFixedBitSet
            LucenePreparedEntityFilter b = buildRange(index, 100); // FixedBitSet
            a.combine(b);
            b.close();
            assertThat(setBits(((LucenePreparedEntityFilter) a).bitSetFor(index.segmentKey(0))))
                    .containsExactly(java.util.stream.IntStream.range(0, 100).toArray());
        }
    }

    private LucenePreparedEntityFilter buildRange(Index index, int count) {
        LuceneEntityFilterBuilder builder =
                new LuceneEntityFilterBuilder(ENTITY_ID_KEY, List.of(index.asRef()), INSTANCE);
        for (long id = 0; id < count; id++) {
            builder.add(id);
        }
        return (LucenePreparedEntityFilter) builder.build();
    }

    private LucenePreparedEntityFilter build(Index index, long... entityIds) {
        LuceneEntityFilterBuilder builder =
                new LuceneEntityFilterBuilder(ENTITY_ID_KEY, List.of(index.asRef()), INSTANCE);
        for (long id : entityIds) {
            builder.add(id);
        }
        return (LucenePreparedEntityFilter) builder.build();
    }

    private static long[] ids(int fromInclusive, int toExclusive) {
        long[] out = new long[toExclusive - fromInclusive];
        for (int i = 0; i < out.length; i++) {
            out[i] = fromInclusive + i;
        }
        return out;
    }

    private static int[] setBits(BitSet bits) {
        return java.util.stream.IntStream.range(0, bits.length())
                .filter(bits::get)
                .toArray();
    }

    private static Index buildIndex(long[]... segments) throws IOException {
        return buildIndex(writer -> {
            for (long[] segment : segments) {
                for (long id : segment) {
                    addDocument(writer, id);
                }
                writer.commit(); // separate commit => separate segment
            }
        });
    }

    private static Index buildIndex(ThrowingConsumer<LuceneIndexWriter, IOException> writes) throws IOException {
        DirectoryFactory factory = LuceneContext.LUCENE_10.directoryFactory().newInMemoryDirectoryFactory();
        LuceneDirectory directory = factory.open(null);
        try (LuceneIndexWriter writer = directory.newWriter(WRITER_CONFIG)) {
            writes.accept(writer);
        }
        LuceneDirectoryReader reader = directory.open();
        LuceneIndexSearcher searcher = reader.newDirectSearcher();
        return new Index(factory, directory, reader, searcher);
    }

    private static void addDocument(LuceneIndexWriter writer, long id) throws IOException {
        LuceneDocument doc = writer.newDocument();
        doc.addStringField(ENTITY_ID_KEY, Long.toString(id), true);
        writer.addDocument(doc);
    }

    /** Rewrite the document for {@code id} the way an entity update does: delete-by-term + add. */
    private static void updateDocument(LuceneIndexWriter writer, long id) throws IOException {
        LuceneDocument doc = writer.newDocument();
        doc.addStringField(ENTITY_ID_KEY, Long.toString(id), true);
        writer.updateDocument(ENTITY_ID_KEY, id, doc);
    }

    private record Accepted(int leaf, int doc, String id, boolean live) {}

    /** Every (leaf, doc) the filter accepts, with the stored entity id and whether the doc is live. */
    private static List<Accepted> accepted(Index index, LucenePreparedEntityFilter filter) throws IOException {
        List<Accepted> out = new ArrayList<>();
        List<LeafReaderContext> contexts = index.searcher().leafContexts();
        for (int leaf = 0; leaf < contexts.size(); leaf++) {
            LeafReader reader = contexts.get(leaf).reader();
            BitSet bits = filter.bitSetFor(reader.getCoreCacheHelper().getKey());
            if (bits == null) {
                continue;
            }
            Bits liveDocs = reader.getLiveDocs();
            StoredFields storedFields = reader.storedFields();
            for (int doc : setBits(bits)) {
                out.add(new Accepted(
                        leaf,
                        doc,
                        storedFields.document(doc).get(ENTITY_ID_KEY),
                        liveDocs == null || liveDocs.get(doc)));
            }
        }
        return out;
    }

    private record Index(
            DirectoryFactory factory,
            LuceneDirectory directory,
            LuceneDirectoryReader reader,
            LuceneIndexSearcher searcher)
            implements AutoCloseable {

        SearcherReference asRef() {
            return new SearcherReference() {
                @Override
                public LuceneIndexSearcher getIndexSearcher() {
                    return searcher;
                }

                @Override
                public void close() {}
            };
        }

        IndexReader.CacheKey segmentKey(int leaf) {
            return searcher.leafContexts()
                    .get(leaf)
                    .reader()
                    .getCoreCacheHelper()
                    .getKey();
        }

        @Override
        public void close() throws Exception {
            searcher.close();
            reader.close();
            directory.close();
            factory.close();
        }
    }
}
