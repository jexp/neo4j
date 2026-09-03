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

import java.util.Arrays;
import org.neo4j.graphdb.Vector;
import org.neo4j.memory.HeapEstimator;

public abstract sealed class AbstractFloat16Vector extends FloatingPointVector permits Float16Vector, BFloat16Vector {
    private static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(Float16Vector.class);

    private final Float16Format format;
    private final short[] coordinates;

    AbstractFloat16Vector(Float16Format format, short... coordinates) {
        this.format = format;
        this.coordinates = coordinates;
    }

    @Override
    public float floatValue(int index) {
        return format.toFloat32(coordinates[index]);
    }

    @Override
    public double doubleValue(int index) {
        return floatValue(index);
    }

    @Override
    public int dimensions() {
        return coordinates.length;
    }

    @Override
    public Vector.CoordinateType coordinateType() {
        return format.coordinateType();
    }

    @Override
    public ValueRepresentation valueRepresentation() {
        return format.valueRepresentation();
    }

    @Override
    public boolean equals(Value other) {
        if (other instanceof AbstractFloat16Vector v) {
            return format.equals(v.format) && Arrays.equals(this.coordinates, v.coordinates);
        }
        return false;
    }

    @Override
    protected int unsafeCompareTo(Value other) {
        final var that = (AbstractFloat16Vector) other;
        if (format != that.format) {
            throw new IllegalStateException(
                    "Comparing two float16 vectors of different formats " + format + " and " + that.format);
        }
        int comparison = Integer.compare(this.dimensions(), that.dimensions());
        if (comparison != 0) {
            return comparison;
        }
        for (int i = 0; i < coordinates.length; i++) {
            comparison = format.compare(coordinates[i], that.coordinates[i]);
            if (comparison != 0) {
                return comparison;
            }
        }
        return 0;
    }

    @Override
    public <E extends Exception> void writeTo(ValueWriter<E> writer) throws E {
        writer.writeFloat16Vector(format, coordinates);
    }

    @Override
    protected long longBits(int i) {
        return coordinates[i];
    }

    @Override
    public String nestedTypeName() {
        return format.nestedTypeName();
    }

    @Override
    protected int computeHashToMemoize() {
        return NumberValues.hash(coordinates);
    }

    @Override
    public long estimatedHeapUsage() {
        return SHALLOW_SIZE + HeapEstimator.sizeOf(coordinates);
    }

    public Float16Format format() {
        return format;
    }

    @Override
    public String toString() {
        return String.format("%s(%s)%s", getTypeName(), nestedTypeName(), Arrays.toString(coordinates));
    }
}
