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
package org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation

import org.neo4j.cypher.internal.runtime.IsNoValue
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.operations.CypherFunctions
import org.neo4j.cypher.operations.PreparedEntityFilterValue
import org.neo4j.exceptions.InternalException
import org.neo4j.internal.kernel.api.EntityFilterBuilder
import org.neo4j.internal.kernel.api.EntityFilterIndexReader
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.AnyValue

class CompileEntityFilterFunction(value: Expression, queryIndexId: Int, memoryTracker: MemoryTracker)
    extends AggregationFunction {

  private[this] var builder: EntityFilterBuilder = _

  private[this] def ensureBuilder(state: QueryState): EntityFilterBuilder = {
    if (builder == null) {
      builder = state.queryIndexes(queryIndexId).reader() match {
        case reader: EntityFilterIndexReader => reader.newEntityFilterBuilder(memoryTracker)
        case reader => throw InternalException.internalError(
            "Invalid IndexReader",
            "Required an EntityFilterIndexReader but got " + reader.getClass.getSimpleName
          )
      }
    }
    builder
  }

  override def apply(data: ReadableRow, state: QueryState): Unit = {
    value(data, state) match {
      case IsNoValue() => onNoValue(state)
      case v           => ensureBuilder(state).add(CypherFunctions.asLong(v))
    }
  }

  override def result(state: QueryState): AnyValue = {
    new PreparedEntityFilterValue(ensureBuilder(state).build())
  }
}
