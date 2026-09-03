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
package org.neo4j.kernel.impl.index.schema;

import static org.neo4j.kernel.impl.index.schema.GenericKey.setCursorException;
import static org.neo4j.kernel.impl.index.schema.Types.SIZE_ARRAY_LENGTH;
import static org.neo4j.values.storable.VectorValue.vectorCoordinateType;
import static org.neo4j.values.storable.VectorValue.vectorCoordinateTypeCode;

import java.util.Arrays;
import java.util.StringJoiner;
import org.neo4j.graphdb.Vector;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageCursorUtil;
import org.neo4j.values.storable.Float16Format;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.ValueWriter;
import org.neo4j.values.storable.VectorValue;

class VectorArrayType extends AbstractArrayType<VectorValue> {
    // Affected key state:
    // long0Array: coordinateType+dimensions <msb>[1B coordinateType],[2B dimensions]<lsb>
    // byteArrayArray

    private static final int SIZE_VECTOR_HEADER = 3;

    VectorArrayType(byte typeId) {
        super(
                ValueGroup.VECTOR_ARRAY,
                typeId,
                new VectorArrayElementComparator(),
                new VectorArrayElementValueFactory(),
                new VectorArrayElementWriter(),
                null,
                VectorValue[]::new,
                ValueWriter.ArrayType.VECTOR);
    }

    @Override
    int valueSize(GenericKey<?> state) {
        int size = SIZE_ARRAY_LENGTH;
        for (int i = 0; i < state.arrayLength; i++) {
            size += SIZE_VECTOR_HEADER + byteLengthOf(state, i);
        }
        return size;
    }

    @Override
    boolean readValue(PageCursor cursor, int size, GenericKey<?> into) {
        short length = cursor.getShort();
        if (!setArrayLengthWhenReading(into, cursor, length)) {
            return false;
        }
        into.beginArray(into.arrayLength, ValueWriter.ArrayType.VECTOR);
        into.long0Array = ensureBigEnough(into.long0Array, into.arrayLength);
        into.byteArrayArray = ensureBigEnough(into.byteArrayArray, into.arrayLength);
        int remaining = size - SIZE_ARRAY_LENGTH;
        for (int i = 0; i < into.arrayLength; i++) {
            long header = PageCursorUtil.get3BInt(cursor) & 0xFFFFFF;
            byte coordinateTypeId = coordinateTypeOf(header);
            Vector.CoordinateType coordinateType = coordinateTypeOrNull(coordinateTypeId);
            if (coordinateType == null) {
                setCursorException(cursor, "non-valid coordinate type for vector array, " + coordinateTypeId);
                return false;
            }
            into.long0Array[i] = header;
            int byteLength = VectorValue.bytesPerDimension(coordinateType) * dimensionsOf(header);
            remaining -= SIZE_VECTOR_HEADER + byteLength;
            if (remaining < 0) {
                setCursorException(cursor, "non-valid vector length for vector array, " + byteLength);
                return false;
            }
            into.byteArrayArray[i] = ensureBigEnough(into.byteArrayArray[i], byteLength);
            cursor.getBytes(into.byteArrayArray[i], 0, byteLength);
        }
        into.endArray();
        return true;
    }

    @Override
    String toString(GenericKey<?> state) {
        /* Don't deserialize the vectors for the string representation, the state may stem from an inconsistent read */
        StringJoiner joiner = new StringJoiner(", ", "VectorArray[", "]");
        for (int i = 0; i < state.arrayLength; i++) {
            long header = state.long0Array[i];
            joiner.add("coordinateType=%d, dimensions=%d".formatted(coordinateTypeOf(header), dimensionsOf(header)));
        }
        return joiner.toString();
    }

    private static byte coordinateTypeOf(long header) {
        return (byte) (header >>> (Short.SIZE));
    }

    private static int dimensionsOf(long header) {
        return (int) (header & 0xFFFF);
    }

    private static int byteLengthOf(GenericKey<?> key, int i) {
        long header = key.long0Array[i];
        int dimensions = dimensionsOf(header);
        Vector.CoordinateType coordinateType = vectorCoordinateType(coordinateTypeOf(header));
        return VectorValue.bytesPerDimension(coordinateType) * dimensions;
    }

    @Override
    void copyValue(GenericKey<?> to, GenericKey<?> from, int arrayLength) {
        to.long0Array = ensureBigEnough(to.long0Array, arrayLength);
        System.arraycopy(from.long0Array, 0, to.long0Array, 0, arrayLength);
        to.byteArrayArray = ensureBigEnough(to.byteArrayArray, arrayLength);
        for (int i = 0; i < arrayLength; i++) {
            to.byteArrayArray[i] = ensureBigEnough(to.byteArrayArray[i], from.byteArrayArray[i].length);
            System.arraycopy(from.byteArrayArray[i], 0, to.byteArrayArray[i], 0, from.byteArrayArray[i].length);
        }
    }

    @Override
    void initializeArray(GenericKey<?> key, int length, ValueWriter.ArrayType arrayType) {
        key.long0Array = ensureBigEnough(key.long0Array, length);
        key.byteArrayArray = ensureBigEnough(key.byteArrayArray, length);
    }

    static void write(GenericKey<?> key, int offset, Vector.CoordinateType coordinateType, int dimension, byte[] data) {
        key.long0Array[offset] = dimension;
        key.long0Array[offset] |= (long) vectorCoordinateTypeCode(coordinateType) << (Short.SIZE);
        key.byteArrayArray[offset] = data;
    }

    private static Vector.CoordinateType coordinateTypeOrNull(byte coordinateType) {
        return vectorCoordinateType(t -> {}, coordinateType);
    }

    private static class VectorArrayElementComparator implements ArrayElementComparator {
        @Override
        public int compare(GenericKey<?> o1, GenericKey<?> o2, int i) {
            // first compare coordinateType, then dimensions, then arrays themselves
            // we can't compare the internal coordinate type ID since coordinate types may have arrived
            // in the product out of order with the order defined by ValueGroup.
            byte coordinateTypeId = coordinateTypeOf(o1.long0Array[i]);
            byte o2CoordinateTypeId = coordinateTypeOf(o2.long0Array[i]);
            if (coordinateTypeId != o2CoordinateTypeId) {
                ValueGroup valueGroup1 = valueGroup(coordinateTypeId);
                ValueGroup valueGroup2 = valueGroup(o2CoordinateTypeId);
                if (valueGroup1 == null || valueGroup2 == null) {
                    return Byte.compare(coordinateTypeId, o2CoordinateTypeId);
                }
                return valueGroup1.compareTo(valueGroup2);
            }
            int dimensionsComparison = Integer.compare(dimensionsOf(o1.long0Array[i]), dimensionsOf(o2.long0Array[i]));
            if (dimensionsComparison != 0) {
                return dimensionsComparison;
            }
            byte[] o1Bytes = o1.byteArrayArray[i];
            byte[] o2Bytes = o2.byteArrayArray[i];
            Vector.CoordinateType coordinateType = coordinateTypeOrNull(coordinateTypeId);
            if (coordinateType == null) {
                // We can not throw here because we will visit this method inside a pageCursor.shouldRetry() block.
                // Just return a comparison that at least will be commutative.
                return Arrays.compare(o1Bytes, o2Bytes);
            }
            int numBytes = byteLengthOf(o1, i);
            return switch (coordinateType) {
                case INTEGER8 -> VectorKeyType.Int8VectorKey.compareBytes(o1Bytes, o2Bytes, numBytes);
                case INTEGER16 -> VectorKeyType.Int16VectorKey.compareBytes(o1Bytes, o2Bytes, numBytes);
                case INTEGER32 -> VectorKeyType.Int32VectorKey.compareBytes(o1Bytes, o2Bytes, numBytes);
                case INTEGER64 -> VectorKeyType.Int64VectorKey.compareBytes(o1Bytes, o2Bytes, numBytes);
                case FLOAT16 ->
                    VectorKeyType.Float16VectorKey.compareBytes(Float16Format.FLOAT16, o1Bytes, o2Bytes, numBytes);
                case BFLOAT16 ->
                    VectorKeyType.Float16VectorKey.compareBytes(Float16Format.BFLOAT16, o1Bytes, o2Bytes, numBytes);
                case FLOAT32 -> VectorKeyType.Float32VectorKey.compareBytes(o1Bytes, o2Bytes, numBytes);
                case FLOAT64 -> VectorKeyType.Float64VectorKey.compareBytes(o1Bytes, o2Bytes, numBytes);
            };
        }

        private ValueGroup valueGroup(byte coordinateTypeId) {
            Vector.CoordinateType coordinateType = vectorCoordinateType(t -> {}, coordinateTypeId);
            if (coordinateType == null) {
                return null;
            }
            return VectorKeyType.fromCoordinateType(coordinateType);
        }
    }

    private static class VectorArrayElementValueFactory implements ArrayElementValueFactory<VectorValue> {
        @Override
        public VectorValue from(GenericKey<?> k, int i) {
            int dimensions = dimensionsOf(k.long0Array[i]);
            return switch (vectorCoordinateType(coordinateTypeOf(k.long0Array[i]))) {
                case INTEGER8 -> VectorKeyType.Int8VectorKey.asValue(dimensions, k.byteArrayArray[i]);
                case INTEGER16 -> VectorKeyType.Int16VectorKey.asValue(dimensions, k.byteArrayArray[i]);
                case INTEGER32 -> VectorKeyType.Int32VectorKey.asValue(dimensions, k.byteArrayArray[i]);
                case INTEGER64 -> VectorKeyType.Int64VectorKey.asValue(dimensions, k.byteArrayArray[i]);
                case FLOAT16 ->
                    VectorKeyType.Float16VectorKey.asValue(dimensions, k.byteArrayArray[i], Float16Format.FLOAT16);
                case BFLOAT16 ->
                    VectorKeyType.Float16VectorKey.asValue(dimensions, k.byteArrayArray[i], Float16Format.BFLOAT16);
                case FLOAT32 -> VectorKeyType.Float32VectorKey.asValue(dimensions, k.byteArrayArray[i]);
                case FLOAT64 -> VectorKeyType.Float64VectorKey.asValue(dimensions, k.byteArrayArray[i]);
            };
        }
    }

    private static class VectorArrayElementWriter implements ArrayElementWriter {
        @Override
        public void write(PageCursor c, GenericKey<?> k, int i) {
            PageCursorUtil.put3BInt(c, (int) k.long0Array[i]);
            int byteLength = byteLengthOf(k, i);
            c.putBytes(k.byteArrayArray[i], 0, byteLength);
        }
    }
}
