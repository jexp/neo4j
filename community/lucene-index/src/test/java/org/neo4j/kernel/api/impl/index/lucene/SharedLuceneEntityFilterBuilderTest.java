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
import java.util.List;
import java.util.stream.IntStream;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.BitSet;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.kernel.api.EntityFilterBuilder;
import org.neo4j.kernel.api.impl.index.SearcherReference;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.test.Race;

/**
 * Correctness of {@link SharedLuceneEntityFilterBuilder}, whose per-worker writers set bits into ONE
 * shared bitset via a lock-free atomic OR. The high-value cases here are the concurrent ones: they must
 * prove the atomic OR never drops a bit when multiple threads touch the same 64-bit word at once.
 */
class SharedLuceneEntityFilterBuilderTest {
    private static final LuceneIndexWriterConfig WRITER_CONFIG =
            LuceneIndexWriterConfig.analyzerOnly(new KeywordAnalyzer());

    @Test
    void singleWriterResolvesIds() throws Exception {
        try (Index index = buildIndex(ids(0, 10))) {
            SharedLuceneEntityFilterBuilder shared =
                    new SharedLuceneEntityFilterBuilder(ENTITY_ID_KEY, List.of(index.asRef()), INSTANCE);
            EntityFilterBuilder writer = shared.newWriter();
            writer.add(2);
            writer.add(5);
            writer.add(7);
            LucenePreparedEntityFilter filter = (LucenePreparedEntityFilter) shared.build();
            assertThat(setBits(filter.bitSetFor(index.segmentKey(0)))).containsExactly(2, 5, 7);
        }
    }

    @Test
    void noWritersProducesEmptyFilter() throws Exception {
        try (Index index = buildIndex(ids(0, 10))) {
            SharedLuceneEntityFilterBuilder shared =
                    new SharedLuceneEntityFilterBuilder(ENTITY_ID_KEY, List.of(index.asRef()), INSTANCE);
            LucenePreparedEntityFilter filter = (LucenePreparedEntityFilter) shared.build();
            BitSet bits = filter.bitSetFor(index.segmentKey(0));
            assertThat(bits == null || bits.cardinality() == 0).isTrue();
        }
    }

    /**
     * The stress case: ids are interleaved across threads (thread t owns t, t+T, t+2T, ...), so adjacent
     * docs -- which live in the SAME 64-bit backing word -- are set by different threads simultaneously.
     * If the atomic OR were racy, a doc would go missing and containsExactly(0..docs-1) would fail.
     */
    @RepeatedTest(30)
    void concurrentInterleavedWritersLoseNoBits() throws Throwable {
        int docs = 4096; // 64 words; every word is written by all threads
        int threads = 8;
        try (Index index = buildIndex(ids(0, docs))) {
            assertThat(index.searcher().leafContexts())
                    .as("test assumes a single segment (entity id == docId)")
                    .hasSize(1);
            SharedLuceneEntityFilterBuilder shared =
                    new SharedLuceneEntityFilterBuilder(ENTITY_ID_KEY, List.of(index.asRef()), INSTANCE);
            runConcurrently(threads, shared, (writer, tid) -> {
                for (long id = tid; id < docs; id += threads) {
                    writer.add(id);
                }
            });
            LucenePreparedEntityFilter filter = (LucenePreparedEntityFilter) shared.build();
            assertThat(setBits(filter.bitSetFor(index.segmentKey(0))))
                    .as("lock-free OR must not drop bits on contended words")
                    .containsExactly(IntStream.range(0, docs).toArray());
        }
    }

    /** Every thread sets the full range: the union must still be exactly the range (OR is idempotent). */
    @RepeatedTest(10)
    void concurrentOverlappingWritersAreIdempotent() throws Throwable {
        int docs = 2000;
        int threads = 6;
        try (Index index = buildIndex(ids(0, docs))) {
            assertThat(index.searcher().leafContexts()).hasSize(1);
            SharedLuceneEntityFilterBuilder shared =
                    new SharedLuceneEntityFilterBuilder(ENTITY_ID_KEY, List.of(index.asRef()), INSTANCE);
            runConcurrently(threads, shared, (writer, tid) -> {
                for (long id = 0; id < docs; id++) {
                    writer.add(id);
                }
            });
            LucenePreparedEntityFilter filter = (LucenePreparedEntityFilter) shared.build();
            assertThat(setBits(filter.bitSetFor(index.segmentKey(0))))
                    .containsExactly(IntStream.range(0, docs).toArray());
        }
    }

    // ---- harness ------------------------------------------------------------------------------------

    @FunctionalInterface
    private interface WriterWork {
        void run(EntityFilterBuilder writer, int threadId) throws Exception;
    }

    /** Runs {@code work} on {@code threads} contestants that Race starts simultaneously, each with its own writer. */
    private static void runConcurrently(int threads, SharedLuceneEntityFilterBuilder shared, WriterWork work)
            throws Throwable {
        Race race = new Race();
        race.addContestants(
                threads,
                tid -> Race.throwing(() -> {
                    EntityFilterBuilder writer = shared.newWriter();
                    work.run(writer, tid);
                }));
        race.go();
    }

    private static long[] ids(int fromInclusive, int toExclusive) {
        long[] out = new long[toExclusive - fromInclusive];
        for (int i = 0; i < out.length; i++) {
            out[i] = fromInclusive + i;
        }
        return out;
    }

    private static int[] setBits(BitSet bits) {
        return IntStream.range(0, bits.length()).filter(bits::get).toArray();
    }

    private static Index buildIndex(long[]... segments) throws IOException {
        DirectoryFactory factory = LuceneContext.LUCENE_10.directoryFactory().newInMemoryDirectoryFactory();
        LuceneDirectory directory = factory.open(null);
        try (LuceneIndexWriter writer = directory.newWriter(WRITER_CONFIG)) {
            for (long[] segment : segments) {
                for (long id : segment) {
                    LuceneDocument doc = writer.newDocument();
                    doc.addStringField(ENTITY_ID_KEY, Long.toString(id), true);
                    writer.addDocument(doc);
                }
                writer.commit(); // separate commit => separate segment
            }
        }
        LuceneDirectoryReader reader = directory.open();
        LuceneIndexSearcher searcher = reader.newDirectSearcher();
        return new Index(factory, directory, reader, searcher);
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
