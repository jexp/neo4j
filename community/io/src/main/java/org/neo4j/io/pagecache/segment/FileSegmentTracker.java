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

import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;

/**
 * A file segment tracker monitors modifications to individual segments of a file and maintains
 * information about which segments have been changed. This interface provides mechanisms to:
 * notify when segments are modified, set the total number of segments in the file, and retrieve
 * all changed segments.
 */
public interface FileSegmentTracker {
    FileSegmentTracker EMPTY_FILE_TRACKER = new FileSegmentTracker() {
        @Override
        public void segmentChanged(int segmentIndex) {}

        @Override
        public void segmentCount(int segmentCount) {}

        @Override
        public int[] drainChangedSegments() {
            return EMPTY_INT_ARRAY;
        }
    };

    void segmentChanged(int segmentIndex);

    void segmentCount(int segmentCount);

    int[] drainChangedSegments();
}
