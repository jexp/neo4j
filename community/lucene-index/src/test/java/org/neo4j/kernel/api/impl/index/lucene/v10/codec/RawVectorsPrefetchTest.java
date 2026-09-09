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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.lucene.codecs.lucene95.HasIndexSlice;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RawVectorsPrefetchTest {
    private static final int DIMENSIONS = 8;
    private static final int VECTOR_BYTES = DIMENSIONS * Float.BYTES;
    private static final int SIZE = 100;

    @Test
    void mergeConsecutiveOrdinalsIntoOneRange() throws IOException {
        assertThat(rangesFor(3, 4, 5)).containsExactly(range(3, 3));
    }

    @Test
    void keepNonAdjacentOrdinalsApart() throws IOException {
        assertThat(rangesFor(3, 9)).containsExactly(range(3, 1), range(9, 1));
    }

    /// Advising the gap would fetch pages nobody asked for, which is the read amplification the
    /// prefetch is meant to remove.
    @Test
    void neverAdviseAcrossAGap() throws IOException {
        assertThat(rangesFor(0, 2)).containsExactly(range(0, 1), range(2, 1));
    }

    @Test
    void sortBeforeAdvising() throws IOException {
        assertThat(rangesFor(9, 3, 4)).containsExactly(range(3, 2), range(9, 1));
    }

    @Test
    void collapseDuplicates() throws IOException {
        assertThat(rangesFor(7, 7, 7, 8)).containsExactly(range(7, 2));
    }

    /// numOrds, not the array length, decides how much of the buffer is live -- the caller reuses an
    /// oversized array.
    @Test
    void honourNumOrds() throws IOException {
        RecordingInput input = new RecordingInput();
        RawVectorsPrefetch.of(new SlicedValues(input)).prefetch(new int[] {1, 2, 40, 41, 0, 0, 0}, 4);
        assertThat(input.ranges).containsExactly(range(1, 2), range(40, 2));
    }

    @Test
    void adviseNothingForASingleVector() throws IOException {
        RecordingInput input = new RecordingInput();
        RawVectorsPrefetch prefetch = RawVectorsPrefetch.of(new SlicedValues(input));
        prefetch.prefetch(new int[] {5}, 1);
        prefetch.prefetch(new int[0], 0);
        assertThat(input.ranges).isEmpty();
    }

    @Test
    void reuseTheSortBufferAcrossCalls() throws IOException {
        RecordingInput input = new RecordingInput();
        RawVectorsPrefetch prefetch = RawVectorsPrefetch.of(new SlicedValues(input));
        prefetch.prefetch(new int[] {50, 51, 52, 60, 70, 80}, 6);
        input.ranges.clear();
        prefetch.prefetch(new int[] {9, 10}, 2);
        assertThat(input.ranges).containsExactly(range(9, 2));
    }

    /// The point of the whole class: the batch must reach the advisor, one call per merged range, and
    /// must *not* also go to Lucene's [IndexInput#prefetch], which throttles itself to under 1% of
    /// the calls it is given. Whether the advisor then reaches `madvise` is
    /// `NativeAccessRawVectorAdvisorTest`'s job -- that one needs a real mapping and native access,
    /// neither of which this assertion needs.
    @Test
    void routeTheBatchToTheAdvisorInsteadOfLucene() throws IOException {
        RecordingInput input = new RecordingInput();
        ArrayList<long[]> advised = new ArrayList<>();
        RawVectorAdvisor advisor = (slice, offset, length) -> {
            assertThat(slice).isSameAs(input);
            advised.add(new long[] {offset, length});
            return true;
        };

        // 3 merged ranges: {1,2}, {40}, {70,71,72}
        RawVectorsPrefetch.of(new SlicedValues(input), advisor).prefetch(new int[] {1, 2, 40, 70, 71, 72}, 6);

        assertThat(advised).containsExactly(range(1, 2), range(40, 1), range(70, 3));
        assertThat(input.ranges)
                .as("Lucene's throttled prefetch must not be used as well")
                .isEmpty();
    }

    /// An advisor that cannot act -- not a mapping, a range across two mmap chunks, no syscall -- must
    /// leave the caller on Lucene's path rather than dropping the prefetch altogether.
    @Test
    void fallBackToLuceneWhenTheAdvisorDeclines() throws IOException {
        RecordingInput input = new RecordingInput();
        RawVectorsPrefetch.of(new SlicedValues(input), (slice, offset, length) -> false)
                .prefetch(new int[] {1, 2, 40}, 3);

        assertThat(input.ranges).containsExactly(range(1, 2), range(40, 1));
    }

    /// A mapped slice on a real directory, which is the arrangement the advisor is built for. Where
    /// native access is unavailable there is no advisor, so the assertion is about the fallback being
    /// taken rather than about the syscall.
    @Test
    void adviseAMappedSlice(@TempDir Path dir) throws IOException {
        assumeThat(MMapDirectory.supportsMadvise()).isTrue();

        try (MMapDirectory directory = new MMapDirectory(dir)) {
            byte[] bytes = new byte[1 << 20];
            new Random(7).nextBytes(bytes);
            try (IndexOutput out = directory.createOutput("vectors.bin", IOContext.DEFAULT)) {
                out.writeBytes(bytes, bytes.length);
            }
            try (IndexInput in = directory.openInput("vectors.bin", IOContext.DEFAULT)) {
                RawVectorsPrefetch prefetch = RawVectorsPrefetch.of(new SlicedValues(in));
                assertThat(prefetch).isNotNull();
                int[] ords = {1, 2, 40, 70, 71, 72};
                assertThatCode(() -> prefetch.prefetch(ords, ords.length)).doesNotThrowAnyException();
            }
        }
    }

    @Test
    void fallBackWhenTheValuesCannotSayWhereTheyRead() {
        assertThat(RawVectorsPrefetch.of(new SlicedValues(null))).isNull();
        assertThat(RawVectorsPrefetch.of(new OpaqueValues())).isNull();
    }

    private static List<long[]> rangesFor(int... ords) throws IOException {
        RecordingInput input = new RecordingInput();
        RawVectorsPrefetch.of(new SlicedValues(input)).prefetch(ords, ords.length);
        return input.ranges;
    }

    private static long[] range(int firstOrd, int vectors) {
        return new long[] {(long) firstOrd * VECTOR_BYTES, (long) vectors * VECTOR_BYTES};
    }

    private static final class RecordingInput extends IndexInput {
        private final List<long[]> ranges = new ArrayList<>();

        RecordingInput() {
            super("recording");
        }

        @Override
        public void prefetch(long offset, long length) {
            ranges.add(new long[] {offset, length});
        }

        @Override
        public void close() {}

        @Override
        public long getFilePointer() {
            return 0;
        }

        @Override
        public void seek(long pos) {}

        @Override
        public long length() {
            return (long) SIZE * VECTOR_BYTES;
        }

        @Override
        public IndexInput slice(String description, long offset, long length) {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte readByte() {
            return 0;
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) {}
    }

    private static class OpaqueValues extends FloatVectorValues {
        @Override
        public float[] vectorValue(int ord) {
            return new float[DIMENSIONS];
        }

        @Override
        public int dimension() {
            return DIMENSIONS;
        }

        @Override
        public int size() {
            return SIZE;
        }

        @Override
        public FloatVectorValues copy() {
            return this;
        }

        @Override
        public DocIndexIterator iterator() {
            return createDenseIterator();
        }

        @Override
        public VectorScorer scorer(float[] target) {
            return null;
        }
    }

    private static final class SlicedValues extends OpaqueValues implements HasIndexSlice {
        private final IndexInput slice;

        SlicedValues(IndexInput slice) {
            this.slice = slice;
        }

        @Override
        public IndexInput getSlice() {
            return slice;
        }
    }
}
