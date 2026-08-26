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
package org.neo4j.kernel.api.impl.index.lucene;

/**
 * Something with in-progress work that can be abandoned part-way through rather than run to completion.
 * <p/>
 * Declared free of any Lucene types so that the per-version implementations, which extend unrelated shaded and
 * unshaded Lucene classes, can share it.
 */
public interface Abortable {
    /**
     * Give up on whatever work is in progress as soon as possible, and do not start any more of it.
     * <p/>
     * Returns without waiting for the work to notice, so callers that need it to have actually stopped must wait for
     * that separately.
     */
    void abort();
}
