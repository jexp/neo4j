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
package org.neo4j.cypher.operations;

import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;

import java.util.Comparator;
import org.neo4j.internal.kernel.api.PreparedEntityFilter;
import org.neo4j.values.AnyValue;
import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.Comparison;
import org.neo4j.values.TernaryComparator;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.VirtualValue;
import org.neo4j.values.virtual.VirtualValueGroup;

/**
 * Carrier value that transports an already-resolved {@link PreparedEntityFilter} from the
 * aggregation ({@code CompileMatchFilterBitsetFunction}) into the vector index seek
 * ({@code PropertyIndexQueries#matchEntitySet}). It is never compared, written, hashed for grouping,
 * or mapped — it only ever flows straight through a single column — so most {@link VirtualValue}
 * operations are unsupported.
 */
public final class PreparedEntityFilterValue extends VirtualValue {
    private static final long SHALLOW_SIZE = shallowSizeOfInstance(PreparedEntityFilterValue.class);

    private final PreparedEntityFilter filter;

    public PreparedEntityFilterValue(PreparedEntityFilter filter) {
        this.filter = filter;
    }

    public PreparedEntityFilter filter() {
        return filter;
    }

    @Override
    public boolean equals(VirtualValue other) {
        return this == other;
    }

    @Override
    public VirtualValueGroup valueGroup() {
        return VirtualValueGroup.LIST;
    }

    @Override
    public int unsafeCompareTo(VirtualValue other, Comparator<AnyValue> comparator) {
        throw new UnsupportedOperationException("PreparedEntityFilterValue is not comparable");
    }

    @Override
    public Comparison unsafeTernaryCompareTo(VirtualValue other, TernaryComparator<AnyValue> comparator) {
        throw new UnsupportedOperationException("PreparedEntityFilterValue is not comparable");
    }

    @Override
    protected int computeHashToMemoize() {
        return System.identityHashCode(this);
    }

    @Override
    public <E extends Exception> void writeTo(AnyValueWriter<E> writer) {
        throw new UnsupportedOperationException("PreparedEntityFilterValue cannot be written");
    }

    @Override
    public <T> T map(ValueMapper<T> mapper) {
        throw new UnsupportedOperationException("PreparedEntityFilterValue cannot be mapped");
    }

    @Override
    public String getTypeName() {
        return "PreparedEntityFilter";
    }

    @Override
    public long estimatedHeapUsage() {
        return SHALLOW_SIZE + filter.heapEstimate();
    }
}
