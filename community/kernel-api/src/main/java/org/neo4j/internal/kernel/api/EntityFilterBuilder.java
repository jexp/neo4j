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
package org.neo4j.internal.kernel.api;

/**
 * Incrementally resolves entity ids into a {@link PreparedEntityFilter} against a specific index
 * snapshot, without retaining the ids themselves. Obtained from {@link IndexReadSession} so it is
 * bound to the exact reader the subsequent seek will use.
 */
public interface EntityFilterBuilder extends AutoCloseable {
    /** Resolve {@code entityId} against the index and record it in the filter under construction. */
    void add(long entityId);

    /** Finish building. The builder must not be used after this. Ownership of tracked heap transfers to the result. */
    PreparedEntityFilter build();

    /** Release resources if {@link #build()} is never called. */
    @Override
    void close();
}
