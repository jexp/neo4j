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
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.operations.CypherCoercions
import org.neo4j.cypher.operations.CypherFunctions
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.MapValueBuilder

case class MapEntriesComprehensionFunction(
  collection: Expression,
  keyVariableName: String,
  keyVariableOffset: Int,
  valueVariableName: String,
  valueVariableOffset: Int,
  predicate: Option[Predicate],
  keyExpression: Expression,
  valueExpression: Expression
) extends NullInNullOutExpression(collection) {

  override def compute(value: AnyValue, row: ReadableRow, state: QueryState): MapValue = {
    val map = CypherCoercions.asMapValueStrict(value)
    val builder = new MapValueBuilder(map.size())
    map.foreach((key: String, entryValue: AnyValue) => {
      state.expressionVariables(keyVariableOffset) = Values.stringValue(key)
      state.expressionVariables(valueVariableOffset) = entryValue
      if (predicate.forall(_.isTrue(row, state))) {
        val computedKey = CypherFunctions.asString(keyExpression(row, state))
        builder.add(computedKey, valueExpression(row, state))
      }
    })
    builder.build()
  }

  def rewrite(f: Expression => Expression): Expression =
    f(MapEntriesComprehensionFunction(
      collection.rewrite(f),
      keyVariableName,
      keyVariableOffset,
      valueVariableName,
      valueVariableOffset,
      predicate.map(_.rewriteAsPredicate(f)),
      keyExpression.rewrite(f),
      valueExpression.rewrite(f)
    ))

  override def children: Seq[Expression] = Seq(collection, keyExpression, valueExpression) ++ predicate.toSeq

  def arguments: Seq[Expression] = Seq(collection)

}
