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

import org.neo4j.graphdb.Vector;

public enum Float16Format {
    /**
     * See <a href="https://en.wikipedia.org/wiki/IEEE_754">IEEE 754</a>
     * Uses 5 bits for exponent and 10 bits for mantissa.
     */
    FLOAT16(Vector.CoordinateType.FLOAT16, ValueRepresentation.FLOAT16_VECTOR) {
        @Override
        public short toFloat16(float value) {
            return Float.floatToFloat16(value);
        }

        @Override
        public float toFloat32(short value) {
            return Float.float16ToFloat(value);
        }

        @Override
        public boolean isFinite(short value) {
            // 0_11111_0000000000 : positive infinity
            // 1_11111_0000000000 : negative infinity
            short infinityPattern = (short) 0b11111_0000000000;
            boolean isInfinite = (value & infinityPattern) == infinityPattern;
            return !isInfinite;
        }

        @Override
        public short minValue() {
            return 1;
        }

        @Override
        public short maxValue() {
            return 0b0_11110_1111111111;
        }

        @Override
        public Float16Vector instantiateVectorValue(short[] coordinates) {
            return new Float16Vector(coordinates);
        }
    },
    /**
     * <a href="https://en.wikipedia.org/wiki/Bfloat16_floating-point_format">bfloat16</a>
     * Uses 8 bits for exponent and 7 bits for mantissa.
     */
    BFLOAT16(Vector.CoordinateType.BFLOAT16, ValueRepresentation.BFLOAT16_VECTOR) {
        @Override
        public short toFloat16(float value) {
            // Step 1: Reinterpret the 32-bit float as a 32-bit unsigned integer
            int bits = Float.floatToIntBits(value);

            // Step 2: Check for NaN to preserve quiet/signaling NaN bits
            // (If exponent bits are all 1s and mantissa is non-zero)
            if ((bits & 0x7F800000) == 0x7F800000 && (bits & 0x007FFFFF) != 0) {
                // Right shift by 16 to get the top 16 bits
                // Set the MSB of the mantissa to ensure it stays a NaN (quiet NaN)
                return (short) ((bits >>> Short.SIZE) | 0x0040);
            }

            // Step 3: Rounding to nearest even
            // Add the rounding constant. We add the 15th bit (0x8000) plus
            // a correction factor based on the 16th bit to handle tie-to-even.
            int roundingBias = 0x7FFF + ((bits >> 16) & 1);
            bits += roundingBias;

            // Step 4: Extract the upper 16 bits
            return (short) (bits >>> Short.SIZE);
        }

        @Override
        public float toFloat32(short value) {
            // Step 1: Cast the 16-bit bfloat16 storage type into a 32-bit unsigned integer
            // This places the 16 bits into the lowest 16 bits of the 32-bit container
            int bits = value & 0xFFFF;

            // Step 2: Shift the bits left by 16 positions
            // This moves the sign, exponent, and mantissa to their correct float32 positions,
            // automatically padding the lower 16 bits of the mantissa with zeros.
            return Float.intBitsToFloat(bits << Short.SIZE);
        }

        @Override
        public boolean isFinite(short value) {
            // 0_11111111_0000000 : positive infinity
            // 1_11111111_0000000 : negative infinity
            short infinityPattern = (short) 0b11111111_0000000;
            boolean isInfinite = (value & infinityPattern) == infinityPattern;
            return !isInfinite;
        }

        @Override
        public short minValue() {
            return 1;
        }

        @Override
        public short maxValue() {
            return 0b0_11111110_1111111;
        }

        @Override
        public BFloat16Vector instantiateVectorValue(short[] coordinates) {
            return new BFloat16Vector(coordinates);
        }
    };

    private final Vector.CoordinateType coordinateType;
    private final ValueRepresentation valueRepresentation;

    Float16Format(Vector.CoordinateType coordinateType, ValueRepresentation valueRepresentation) {
        this.coordinateType = coordinateType;
        this.valueRepresentation = valueRepresentation;
    }

    public abstract short toFloat16(float value);

    public abstract float toFloat32(short value);

    public abstract boolean isFinite(short value);

    public abstract short minValue();

    public abstract short maxValue();

    public short negative(short value) {
        return (short) (0x8000 | (value & 0x7FFF));
    }

    public short positive(short value) {
        return (short) (value & 0x7FFF);
    }

    public int compare(short c1, short c2) {
        return Short.compareUnsigned(toOrderableShort(c1), toOrderableShort(c2));
    }

    public String nestedTypeName() {
        return coordinateType.name();
    }

    public Vector.CoordinateType coordinateType() {
        return coordinateType;
    }

    public abstract AbstractFloat16Vector instantiateVectorValue(short[] coordinates);

    /**
     * Makes a float16 value bit-wise comparable with another float16 value.
     */
    short toOrderableShort(short float16) {
        if ((float16 & 0x8000) != 0) {
            return (short) (~float16);
        } else {
            return (short) (float16 ^ 0x8000);
        }
    }

    public short[] toFloat16(float[] array) {
        short[] result = new short[array.length];
        for (int i = 0; i < array.length; i++) {
            result[i] = toFloat16(array[i]);
        }
        return result;
    }

    public float[] toFloat32(short[] array) {
        float[] result = new float[array.length];
        for (int i = 0; i < array.length; i++) {
            result[i] = toFloat32(array[i]);
        }
        return result;
    }

    public ValueRepresentation valueRepresentation() {
        return valueRepresentation;
    }
}
