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
package org.neo4j.wal.enveloped;

import java.io.IOException;

/**
 * Utility class for performing binary search over data sources
 * where values are accessed through a random-access reader, possibly backed by disk.
 *
 * <p>This is conceptually similar to {@link java.util.Arrays#binarySearch(Object[], Object)}
 * but supports lazy reading through a {@link BinarySearchReader} abstraction that may involve
 * I/O (e.g. accessing logs on disk).</p>
 *
 */
public class LogBinarySearch {

    /**
     * Abstraction representing a random-access indexed data source with comparison capabilities.
     *
     */
    public interface BinarySearchReader {
        /**
         * @return the number of searchable elements
         */
        int size();

        /**
         * Returns the value at the given index.
         *
         * @param index index to read
         * @return the value at the index
         */
        long get(int index);

        /**
         * Compares the given search target with a value retrieved during the search.
         *
         * @param searchIndex the index to search for from the reader (e.g version file index for log files)
         * @param target the target value being searched for
         * @return a negative integer if the target is less than the search index,
         *         zero if they are equal,
         *         or a positive integer if the target is greater than the search index
         */
        int compare(long searchIndex, long target);
    }

    /**
     * Performs a binary search over a {@link BinarySearchReader}.
     *
     * @param reader the binary search reader interface to access values
     * @param target the value to search for
     * @return the matching value, or the closest lesser value, or {@code null} if not found
     * @throws IOException if reading values fails
     */
    public static long binarySearch(BinarySearchReader reader, long target) throws IOException {
        int size = reader.size();
        if (size <= 0) {
            throw new IllegalStateException("Reader must have at least one searchable element");
        }

        int low = 0;
        int high = size - 1;
        int lowerResultIndex = -1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            long midValue = reader.get(mid);
            int cmp = reader.compare(midValue, target);

            if (cmp < 0) {
                lowerResultIndex = mid;
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                return midValue;
            }
        }

        if (lowerResultIndex == -1) {
            return -1;
        }
        return reader.get(lowerResultIndex);
    }
}
