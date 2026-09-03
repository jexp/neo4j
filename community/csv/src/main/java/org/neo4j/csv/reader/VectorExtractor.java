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
package org.neo4j.csv.reader;

import static org.neo4j.values.storable.Value.QUOTES_PATTERN;
import static org.neo4j.values.storable.VectorValue.MAX_VECTOR_DIMENSIONS;
import static org.neo4j.values.storable.VectorValue.MIN_VECTOR_DIMENSIONS;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import org.neo4j.graphdb.Vector;
import org.neo4j.values.storable.CSVHeaderInformation;
import org.neo4j.values.storable.Values;
import org.neo4j.values.storable.VectorValue;

///  An extractor of vector values
public interface VectorExtractor<T> extends Extractor<T> {

    String COL_NAME = "VECTOR";

    /// Get an [Extractor] of the same coordinate type, but verifying that
    /// each extracted value has the right dimensionality.
    /// The returned [Extractor] throws an exception when extracting a value where
    /// its dimensionality is not equal to `expectedDimensions`.
    Extractor<T> getDimensionVerifyingExtractor(int expectedDimensions);

    @Override
    default boolean isEmpty(Object value) {
        return value == null || value == Values.NO_VALUE || (value instanceof VectorValue v && v.dimensions() == 0);
    }

    class VectorCSVHeaderInformation implements CSVHeaderInformation {
        private Vector.CoordinateType coordinateType;
        private Integer dimensions;

        public Vector.CoordinateType getCoordinateType() {
            if (coordinateType == null) {
                throw new IllegalArgumentException(
                        "vector must specify coordinate type, e.g.\"v:vector{dimensions:10, coordinateType:byte}\"");
            }
            return coordinateType;
        }

        public int getDimensions() {
            if (dimensions == null) {
                throw new IllegalArgumentException(
                        "vector must specify dimensions, e.g.\"v:vector{dimensions:10, coordinateType:byte}\"");
            }
            return dimensions;
        }

        @Override
        public void assign(String key, Object value) {
            if (!(value instanceof String strValue)) {
                throw new IllegalStateException(
                        "Provided non-String value, that is not supported: %s".formatted(String.valueOf(value)));
            }

            switch (key.toLowerCase(Locale.ROOT)) {
                case "coordinatetype" -> {
                    checkUnassigned("coordinateType", coordinateType);
                    final String normalized =
                            QUOTES_PATTERN.matcher(strValue).replaceAll("").toLowerCase(Locale.ROOT);
                    coordinateType = switch (normalized) {
                        case "byte" -> Vector.CoordinateType.INTEGER8;
                        case "short" -> Vector.CoordinateType.INTEGER16;
                        case "int" -> Vector.CoordinateType.INTEGER32;
                        case "long" -> Vector.CoordinateType.INTEGER64;
                        case "float16" -> Vector.CoordinateType.FLOAT16;
                        case "bfloat16" -> Vector.CoordinateType.BFLOAT16;
                        case "float" -> Vector.CoordinateType.FLOAT32;
                        case "double" -> Vector.CoordinateType.FLOAT64;
                        default ->
                            throw new IllegalArgumentException(
                                    "%s is not a valid coordinate type.".formatted(strValue));
                    };
                }

                case "dimensions" -> {
                    checkUnassigned("dimensions", dimensions);
                    int result;
                    try {
                        result = Integer.parseInt(strValue);
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException(
                                "%s is not a valid value for dimensions.".formatted(strValue));
                    }
                    dimensions = result;
                    if (dimensions < MIN_VECTOR_DIMENSIONS || dimensions > MAX_VECTOR_DIMENSIONS) {
                        throw new IllegalArgumentException("Invalid vector dimensions: " + dimensions);
                    }
                }
            }
        }

        private static void checkUnassigned(String key, Object value) {
            if (value != null) {
                throw new IllegalArgumentException("Duplicate field '%s'".formatted(key));
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            VectorCSVHeaderInformation that = (VectorCSVHeaderInformation) o;
            return Objects.equals(dimensions, that.dimensions) && Objects.equals(coordinateType, that.coordinateType);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dimensions, coordinateType);
        }
    }

    static VectorCSVHeaderInformation parseHeaderInformation(Map<String, String> options) {
        VectorCSVHeaderInformation fields = new VectorCSVHeaderInformation();
        options.forEach(fields::assign);
        return fields;
    }
}
