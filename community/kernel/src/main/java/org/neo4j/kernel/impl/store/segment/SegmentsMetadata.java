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
package org.neo4j.kernel.impl.store.segment;

import static java.util.Collections.emptyList;
import static org.neo4j.storageengine.AppendIndexProvider.UNKNOWN_APPEND_INDEX;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.neo4j.io.pagecache.segment.FileSegmentMetadata;

public record SegmentsMetadata(long lastCheckpointAppendIndex, Collection<FileSegmentsMetadata> filesMetadata) {
    public static final SegmentsMetadata EMPTY_SEGMENTS_METADATA =
            new SegmentsMetadata(UNKNOWN_APPEND_INDEX, emptyList());

    public SegmentsMetadata combine(long checkpointAppendIndex, Collection<FileSegmentMetadata> changedSegments) {
        assert checkpointAppendIndex >= lastCheckpointAppendIndex;
        Map<String, long[]> previousFileMarks = new HashMap<>();
        for (var fileMetadata : filesMetadata) {
            previousFileMarks.put(fileMetadata.name(), fileMetadata.segmentAppendIndexes());
        }

        List<FileSegmentsMetadata> combined = new ArrayList<>(changedSegments.size());
        for (var changes : changedSegments) {
            int[] segments = changes.changesSegments();
            long[] previousMarks = previousFileMarks.get(changes.name());
            long[] marks;
            if (previousMarks == null) {
                marks = new long[Math.max(changes.segmentCount(), requiredLength(segments))];
                Arrays.fill(marks, checkpointAppendIndex);
            } else {
                marks = grow(
                        previousMarks,
                        Math.max(changes.segmentCount(), requiredLength(segments)),
                        checkpointAppendIndex);
                for (int segment : segments) {
                    marks[segment] = checkpointAppendIndex;
                }
            }
            combined.add(new FileSegmentsMetadata(changes.name(), marks));
        }
        return new SegmentsMetadata(checkpointAppendIndex, combined);
    }

    private static int requiredLength(int[] changedSegments) {
        int length = 0;
        for (int segment : changedSegments) {
            length = Math.max(length, segment + 1);
        }
        return length;
    }

    private static long[] grow(long[] marks, int requiredLength, long checkpointAppendIndex) {
        if (requiredLength <= marks.length) {
            return Arrays.copyOf(marks, requiredLength);
        }
        long[] grown = Arrays.copyOf(marks, requiredLength);
        Arrays.fill(grown, marks.length, requiredLength, checkpointAppendIndex);
        return grown;
    }
}
