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
package org.neo4j.values.storable;

import org.apache.commons.lang3.function.ByteConsumer;
import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.graphdb.Vector;
import org.neo4j.hashing.HashFunction;
import org.neo4j.values.Comparison;
import org.neo4j.values.VectorCandidate;
import org.neo4j.values.utils.PrettyPrinter;

public abstract sealed class VectorValue extends HashMemoizingScalarValue
        implements Vector, VectorCandidate, Comparable<VectorValue> permits IntegralVector, FloatingPointVector {

    public static final int MIN_VECTOR_DIMENSIONS = 1;
    public static final int MAX_VECTOR_DIMENSIONS = 4096;

    @Override
    public Vector asObjectCopy() {
        // Similar to PointValue, all VectorValues implement the public interface Vector,
        // which is the Java representation.
        return this;
    }

    @Override
    public String prettyPrint() {
        PrettyPrinter pp = new PrettyPrinter();
        writeTo(pp);
        return pp.value();
    }

    @Override
    public Comparison unsafeTernaryCompareTo(Value otherValue) {
        // Vector values are not comparable under Comparability semantics,
        // unless they are equal.
        if (equals(otherValue)) {
            return Comparison.EQUAL;
        } else {
            return Comparison.UNDEFINED;
        }
    }

    @Override
    public int compareTo(VectorValue that) {
        return Values.COMPARATOR.compare(this, that);
    }

    @Override
    public boolean isIncomparableType() {
        return true;
    }

    /**
     * In order to facilitate implementing {@link #updateHash(HashFunction, long)}.
     *
     * @param i the index of the coordinate
     * @return the long bit representation of the coordinate.
     */
    protected abstract long longBits(int i);

    public abstract String nestedTypeName();

    @Override
    public long updateHash(HashFunction hashFunction, long hash) {
        int len = dimensions();
        hash = hashFunction.update(hash, len);
        for (int i = 0; i < len; i++) {
            hash = hashFunction.update(hash, longBits(i));
        }
        return hash;
    }

    public static void ensureValidDimensions(int dimensions) {
        if (dimensions < VectorValue.MIN_VECTOR_DIMENSIONS || dimensions > VectorValue.MAX_VECTOR_DIMENSIONS) {
            throw InvalidArgumentException.invalidVectorDimensions(
                    VectorValue.MIN_VECTOR_DIMENSIONS, VectorValue.MAX_VECTOR_DIMENSIONS, dimensions);
        }
    }

    public static void ensureFiniteCoordinates(float[] coordinates) {
        for (float c : coordinates) {
            if (!Float.isFinite(c)) {
                throw InvalidArgumentException.invalidVectorCoordinate(coordinates);
            }
        }
    }

    public static void ensureFiniteCoordinates(double[] coordinates) {
        for (double c : coordinates) {
            if (!Double.isFinite(c)) {
                throw InvalidArgumentException.invalidVectorCoordinate(coordinates);
            }
        }
    }

    public static void ensureFiniteCoordinates(Float16Format format, short[] coordinates) {
        for (short c : coordinates) {
            if (!format.isFinite(c)) {
                throw InvalidArgumentException.invalidVectorCoordinate(format.toFloat32(coordinates));
            }
        }
    }

    public static int bytesPerDimension(Vector.CoordinateType coordinateType) {
        return switch (coordinateType) {
            case INTEGER8 -> Byte.BYTES;
            case INTEGER16 -> Short.BYTES;
            case INTEGER32 -> Integer.BYTES;
            case INTEGER64 -> Long.BYTES;
            case FLOAT16, BFLOAT16 -> Short.BYTES;
            case FLOAT32 -> Float.BYTES;
            case FLOAT64 -> Double.BYTES;
        };
    }

    /**
     * This is to not rely on {@link Enum#ordinal()} and making clear that
     * we do not expect more than what can fit into a byte. And also we don't want this mapping to
     * exist in {@link org.neo4j.graphdb.Vector.CoordinateType} itself because it's public API.
     */
    public static byte vectorCoordinateTypeCode(Vector.CoordinateType coordinateType) {
        return switch (coordinateType) {
            case INTEGER8 -> 0;
            case INTEGER16 -> 1;
            case INTEGER32 -> 2;
            case INTEGER64 -> 3;
            case FLOAT32 -> 4;
            case FLOAT64 -> 5;
            // FLOAT16 arrived later
            case FLOAT16 -> 6;
            case BFLOAT16 -> 7;
        };
    }

    public static Vector.CoordinateType vectorCoordinateType(ByteConsumer errorConsumer, byte coordinateType) {
        return switch (coordinateType) {
            case 0 -> Vector.CoordinateType.INTEGER8;
            case 1 -> Vector.CoordinateType.INTEGER16;
            case 2 -> Vector.CoordinateType.INTEGER32;
            case 3 -> Vector.CoordinateType.INTEGER64;
            case 4 -> Vector.CoordinateType.FLOAT32;
            case 5 -> Vector.CoordinateType.FLOAT64;
            case 6 -> Vector.CoordinateType.FLOAT16;
            case 7 -> Vector.CoordinateType.BFLOAT16;
            default -> {
                errorConsumer.accept(coordinateType);
                yield null;
            }
        };
    }

    public static Vector.CoordinateType vectorCoordinateType(byte coordinateType) {
        return vectorCoordinateType(
                t -> {
                    throw new IllegalStateException("Unknown vector coordinate type code: " + t);
                },
                coordinateType);
    }
}
