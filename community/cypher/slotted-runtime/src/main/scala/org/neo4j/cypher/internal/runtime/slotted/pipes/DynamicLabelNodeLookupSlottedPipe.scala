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
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.expressions.PropertyKeyToken
import org.neo4j.cypher.internal.logical.plans.DynamicElement
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.DynamicLabelNodeLookupBase
import org.neo4j.cypher.internal.runtime.interpreted.pipes.DynamicLabelNodeLookupIterator
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.attribution.Id

import scala.util.chaining.scalaUtilChainingOps

case class DynamicLabelNodeLookupSlottedPipe(
  nodeOffset: Int,
  labelExpr: Expression,
  operator: DynamicElement.SetOperator,
  propertyExpressions: Map[PropertyKeyToken, Expression],
  readOnly: Boolean,
  includeChangesFromThisTransaction: Boolean
)(val id: Id = Id.INVALID_ID) extends Pipe {

  protected def internalCreateResults(state: QueryState): ClosingIterator[CypherRow] = {
    val context = state.newRowWithArgument(rowFactory)
    val propertyLookups = DynamicLabelNodeLookupBase.mapPropertyLookups(propertyExpressions, _.apply(context, state))
    DynamicLabelNodeLookupIterator(
      state,
      labelExpr.apply(context, state),
      propertyLookups,
      operator,
      readOnly,
      includeChangesFromThisTransaction
    )
      .toIterator(n => state.newRowWithArgument(rowFactory).tap(_.setLongAt(nodeOffset, n)))
  }
}
