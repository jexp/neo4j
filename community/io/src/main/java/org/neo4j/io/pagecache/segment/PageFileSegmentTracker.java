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
package org.neo4j.io.pagecache.segment;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.impl.factory.primitive.IntLists;

final class PageFileSegmentTracker implements FileSegmentTracker {
    private static final int WORDS_PER_CHUNK = 16;
    private static final int SEGMENTS_PER_CHUNK = WORDS_PER_CHUNK * Long.SIZE;
    private static final int CHUNK_SHIFT = Integer.numberOfTrailingZeros(SEGMENTS_PER_CHUNK);
    private static final int WORD_SHIFT = Integer.numberOfTrailingZeros(Long.SIZE);
    private static final int WORD_INDEX_MASK = WORDS_PER_CHUNK - 1;

    private static final VarHandle WORD_ARRAY = MethodHandles.arrayElementVarHandle(long[].class);
    private static final VarHandle CHUNK_ARRAY = MethodHandles.arrayElementVarHandle(long[][].class);
    private static final VarHandle CHUNKS;

    static {
        try {
            CHUNKS = MethodHandles.lookup().findVarHandle(PageFileSegmentTracker.class, "chunks", long[][].class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private long[][] chunks = new long[1][];
    private volatile int segmentCount;

    @Override
    public void segmentCount(int segmentCount) {
        this.segmentCount = segmentCount;
    }

    int segmentCount() {
        return segmentCount;
    }

    @Override
    public void segmentChanged(int segmentIndex) {
        long[] chunk = chunkFor(segmentIndex >>> CHUNK_SHIFT);
        int wordIndex = (segmentIndex >>> WORD_SHIFT) & WORD_INDEX_MASK;
        long bit = 1L << segmentIndex;
        WORD_ARRAY.getAndBitwiseOr(chunk, wordIndex, bit);
    }

    private long[] chunkFor(int chunkIndex) {
        long[][] outer = (long[][]) CHUNKS.getAcquire(this);
        if (chunkIndex < outer.length) {
            long[] chunk = (long[]) CHUNK_ARRAY.getAcquire(outer, chunkIndex);
            if (chunk != null) {
                return chunk;
            }
        }
        return createChunk(chunkIndex);
    }

    private synchronized long[] createChunk(int chunkIndex) {
        long[][] outer = chunks;
        if (chunkIndex >= outer.length) {
            long[][] grown = new long[Math.max(chunkIndex + 1, outer.length * 2)][];
            System.arraycopy(outer, 0, grown, 0, outer.length);
            CHUNKS.setRelease(this, grown);
            outer = grown;
        }
        long[] chunk = outer[chunkIndex];
        if (chunk == null) {
            chunk = new long[WORDS_PER_CHUNK];
            CHUNK_ARRAY.setRelease(outer, chunkIndex, chunk);
        }
        return chunk;
    }

    @Override
    public int[] drainChangedSegments() {
        long[][] outer = (long[][]) CHUNKS.getAcquire(this);
        MutableIntList dirty = IntLists.mutable.empty();
        for (int chunkIndex = 0; chunkIndex < outer.length; chunkIndex++) {
            long[] chunk = (long[]) CHUNK_ARRAY.getAcquire(outer, chunkIndex);
            if (chunk == null) {
                continue;
            }
            for (int wordIndex = 0; wordIndex < WORDS_PER_CHUNK; wordIndex++) {
                long bits = (long) WORD_ARRAY.getAndSet(chunk, wordIndex, 0L);
                while (bits != 0) {
                    dirty.add(
                            (chunkIndex << CHUNK_SHIFT) | (wordIndex << WORD_SHIFT) | Long.numberOfTrailingZeros(bits));
                    bits &= bits - 1;
                }
            }
        }
        return dirty.toArray();
    }
}
