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

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.kernel.impl.store.segment.SegmentsMetadata.EMPTY_SEGMENTS_METADATA;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.io.pagecache.segment.FileSegmentMetadata;

class SegmentsMetadataTest {
    private static final long FIRST_APPEND_INDEX = 5;
    private static final long SECOND_APPEND_INDEX = 9;

    @Test
    void emptyProfileMarksAllSegmentsOfChangedFile() {
        var combined = EMPTY_SEGMENTS_METADATA.combine(FIRST_APPEND_INDEX, List.of(changes("file", 3, 1)));

        assertThat(combined.lastCheckpointAppendIndex()).isEqualTo(FIRST_APPEND_INDEX);
        assertThat(marksOf(combined, "file"))
                .containsExactly(FIRST_APPEND_INDEX, FIRST_APPEND_INDEX, FIRST_APPEND_INDEX);
    }

    @Test
    void emptyProfileMarksAllSegmentsOfUnchangedFile() {
        var combined = EMPTY_SEGMENTS_METADATA.combine(FIRST_APPEND_INDEX, List.of(changes("file", 3)));

        assertThat(marksOf(combined, "file"))
                .containsExactly(FIRST_APPEND_INDEX, FIRST_APPEND_INDEX, FIRST_APPEND_INDEX);
    }

    @Test
    void emptyProfileCoversChangedSegmentsBeyondSegmentCount() {
        var combined = EMPTY_SEGMENTS_METADATA.combine(FIRST_APPEND_INDEX, List.of(changes("file", 2, 3)));

        assertThat(marksOf(combined, "file"))
                .containsExactly(FIRST_APPEND_INDEX, FIRST_APPEND_INDEX, FIRST_APPEND_INDEX, FIRST_APPEND_INDEX);
    }

    @Test
    void existingProfileMarksOnlyChangedSegment() {
        var previous = profile(file("file", FIRST_APPEND_INDEX, FIRST_APPEND_INDEX, FIRST_APPEND_INDEX));

        var combined = previous.combine(SECOND_APPEND_INDEX, List.of(changes("file", 3, 2)));

        assertThat(combined.lastCheckpointAppendIndex()).isEqualTo(SECOND_APPEND_INDEX);
        assertThat(marksOf(combined, "file"))
                .containsExactly(FIRST_APPEND_INDEX, FIRST_APPEND_INDEX, SECOND_APPEND_INDEX);
    }

    @Test
    void existingProfileMarksScatteredChangedSegments() {
        var previous =
                profile(file("file", FIRST_APPEND_INDEX, FIRST_APPEND_INDEX, FIRST_APPEND_INDEX, FIRST_APPEND_INDEX));

        var combined = previous.combine(SECOND_APPEND_INDEX, List.of(changes("file", 4, 0, 2)));

        assertThat(marksOf(combined, "file"))
                .containsExactly(SECOND_APPEND_INDEX, FIRST_APPEND_INDEX, SECOND_APPEND_INDEX, FIRST_APPEND_INDEX);
    }

    @Test
    void existingProfileKeptForUnchangedFile() {
        var previous = profile(file("file", FIRST_APPEND_INDEX, FIRST_APPEND_INDEX, FIRST_APPEND_INDEX));

        var combined = previous.combine(SECOND_APPEND_INDEX, List.of(changes("file", 3)));

        assertThat(combined.lastCheckpointAppendIndex()).isEqualTo(SECOND_APPEND_INDEX);
        assertThat(marksOf(combined, "file"))
                .containsExactly(FIRST_APPEND_INDEX, FIRST_APPEND_INDEX, FIRST_APPEND_INDEX);
    }

    @Test
    void removeDeletedFileAbsentFromChanges() {
        var previous = profile(file("file", FIRST_APPEND_INDEX, FIRST_APPEND_INDEX));

        var combined = previous.combine(SECOND_APPEND_INDEX, List.of());

        assertThat(combined.lastCheckpointAppendIndex()).isEqualTo(SECOND_APPEND_INDEX);
        assertThat(combined.filesMetadata()).isEmpty();
    }

    @Test
    void existingProfileGrowsForNewSegments() {
        var previous = profile(file("file", FIRST_APPEND_INDEX, FIRST_APPEND_INDEX));

        var combined = previous.combine(SECOND_APPEND_INDEX, List.of(changes("file", 4, 3)));

        // segment 2 is observed for the first time and marked together with the changed segment 3
        assertThat(marksOf(combined, "file"))
                .containsExactly(FIRST_APPEND_INDEX, FIRST_APPEND_INDEX, SECOND_APPEND_INDEX, SECOND_APPEND_INDEX);
    }

    @Test
    void combineFilesWithAndWithoutHistory() {
        var previous = profile(
                file("changed", FIRST_APPEND_INDEX, FIRST_APPEND_INDEX),
                file("deleted", FIRST_APPEND_INDEX, FIRST_APPEND_INDEX));

        var combined = previous.combine(SECOND_APPEND_INDEX, List.of(changes("changed", 2, 1), changes("added", 2, 0)));

        assertThat(marksOf(combined, "changed")).containsExactly(FIRST_APPEND_INDEX, SECOND_APPEND_INDEX);
        assertThat(marksOf(combined, "added")).containsExactly(SECOND_APPEND_INDEX, SECOND_APPEND_INDEX);
        assertThat(combined.filesMetadata()).noneMatch(file -> file.name().equals("deleted"));
    }

    @Test
    void combineSequenceOfProfiles() {
        var first = EMPTY_SEGMENTS_METADATA.combine(FIRST_APPEND_INDEX, List.of(changes("file", 3, 0)));
        var second = first.combine(SECOND_APPEND_INDEX, List.of(changes("file", 3, 1)));

        assertThat(marksOf(second, "file"))
                .containsExactly(FIRST_APPEND_INDEX, SECOND_APPEND_INDEX, FIRST_APPEND_INDEX);
    }

    private static SegmentsMetadata profile(FileSegmentsMetadata... files) {
        return new SegmentsMetadata(FIRST_APPEND_INDEX, List.of(files));
    }

    private static FileSegmentsMetadata file(String name, long... segmentAppendIndexes) {
        return new FileSegmentsMetadata(name, segmentAppendIndexes);
    }

    private static FileSegmentMetadata changes(String name, int segmentCount, int... changedSegments) {
        return new FileSegmentMetadata(name, segmentCount, changedSegments);
    }

    private static long[] marksOf(SegmentsMetadata metadata, String name) {
        return metadata.filesMetadata().stream()
                .filter(file -> file.name().equals(name))
                .findFirst()
                .orElseThrow()
                .segmentAppendIndexes();
    }
}
