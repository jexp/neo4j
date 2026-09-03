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
package org.neo4j.graphdb;

import org.neo4j.annotations.api.PublicApi;

/**
 * A vector is defined by a list of coordinates of a certain {@link CoordinateType}.
 * <p>
 * A vector is different from a {@link org.neo4j.graphdb.spatial.Geometry}:
 * <ul>
 *  <li>A vector must always have at least one dimension and can have up to the maximum number of dimensions supported by this specific neo4j version.</li>
 *  <li>A vector does not necessarily describe a spatial point.</li>
 *  <li>There is no way to get specific coordinates of a vector, it can only be asked for its {@link #dimensions()}.</li>
 * </ul>
 */
@PublicApi
public interface Vector {
    /**
     * The number of coordinates of this vector.
     *
     * @return The number of coordinates of this vector.
     */
    int dimensions();

    /**
     * The coordinate type of this vector.
     *
     * @return The coordinate type of this vector.
     */
    CoordinateType coordinateType();

    /**
     * A CoordinateType can be any of the following:
     * <ul>
     *  <li>INTEGER8: A coordinate type corresponding to a <code>byte</code>.</li>
     *  <li>INTEGER16: A coordinate type corresponding to a <code>short</code>.</li>
     *  <li>INTEGER32: A coordinate type corresponding to an <code>int</code>.</li>
     *  <li>INTEGER64: A coordinate type corresponding to a <code>long</code>.</li>
     *  <li>FLOAT16: A coordinate type corresponding to a <code>float16 (2-byte float)</code>.</li>
     *  <li>BFLOAT16: A coordinate type corresponding to a <code>brainfloat16 (2-byte float)</code>.</li>
     *  <li>FLOAT32: A coordinate type corresponding to a <code>float</code>.</li>
     *  <li>FLOAT64: A coordinate type corresponding to a <code>double</code>.</li>
     * </ul>
     */
    enum CoordinateType {
        INTEGER8("INTEGER8 NOT NULL"),
        INTEGER16("INTEGER16 NOT NULL"),
        INTEGER32("INTEGER32 NOT NULL"),
        INTEGER64("INTEGER NOT NULL"),
        FLOAT16("FLOAT16 NOT NULL"),
        BFLOAT16("BFLOAT16 NOT NULL"),
        FLOAT32("FLOAT32 NOT NULL"),
        FLOAT64("FLOAT NOT NULL");

        private final String normalizedCypherString;

        CoordinateType(String normalizedCypherString) {
            this.normalizedCypherString = normalizedCypherString;
        }

        public String normalizedCypherString() {
            return normalizedCypherString;
        }
    }
}
