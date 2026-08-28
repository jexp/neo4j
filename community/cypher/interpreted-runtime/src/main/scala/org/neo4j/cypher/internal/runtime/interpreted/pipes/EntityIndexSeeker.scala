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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.IndexOrderDescending
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.QueryExpression
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.QueryExpressionSupport
import org.neo4j.cypher.internal.runtime.QueryExpressionSupport.CompiledQueryExpression
import org.neo4j.cypher.internal.runtime.QueryExpressionSupport.ValueResolver
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.cursors.CompositeValueIndexCursor
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.exceptions.InternalException
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.internal.kernel.api.NodeValueIndexCursor
import org.neo4j.internal.kernel.api.PropertyIndexQuery
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor
import org.neo4j.values.AnyValue

/**
 * Class with functionality for executing logical index queries.
 *
 * Maps the logical IndexSeekMode and QueryExpression into the kernel IndexQuery classes, which
 * are passed to the QueryContext for executing the index seek.
 */
class EntityIndexSeeker(
  val indexMode: IndexSeekMode,
  val valueExpr: QueryExpression[Expression],
  val propertyIds: Array[Int]
) {

  def indexSeek(
    state: QueryState,
    index: IndexReadSession,
    needsValues: Boolean,
    indexOrder: IndexOrder,
    baseContext: CypherRow,
    includeChangesFromThisTransaction: Boolean
  ): NodeValueIndexCursor =
    indexMode match {
      case NonLockingSeek =>
        val indexQueries: collection.Seq[Seq[PropertyIndexQuery]] = computeIndexQueries(state, baseContext)
        if (indexQueries.size == 1) {
          state.query.nodeIndexSeek(
            index,
            needsValues,
            indexOrder,
            indexQueries.head,
            includeChangesFromThisTransaction
          )
        } else {
          orderedCursor(
            indexOrder,
            indexQueries.map(query =>
              state.query.nodeIndexSeek(
                index,
                needsValues = needsValues || indexOrder != IndexOrderNone,
                indexOrder,
                query,
                includeChangesFromThisTransaction
              )
            ).toArray
          )
        }

      case LockingUniqueIndexSeek =>
        val indexQueries = computeExactQueries(state, baseContext)
        if (indexQueries.size == 1) {
          state.query.nodeLockingUniqueIndexSeek(index, indexQueries.head)
        } else {
          orderedCursor(
            indexOrder,
            indexQueries.map(query => state.query.nodeLockingUniqueIndexSeek(index, query)).toArray
          )
        }
    }

  def relationshipIndexSeek(
    state: QueryState,
    index: IndexReadSession,
    needsValues: Boolean,
    indexOrder: IndexOrder,
    baseContext: CypherRow,
    includeChangesFromThisTransaction: Boolean
  ): RelationshipValueIndexCursor = indexMode match {
    case NonLockingSeek =>
      val indexQueries: collection.Seq[Seq[PropertyIndexQuery]] = computeIndexQueries(state, baseContext)
      if (indexQueries.size == 1) {
        state.query.relationshipIndexSeek(
          index,
          needsValues,
          indexOrder,
          indexQueries.head,
          includeChangesFromThisTransaction
        )
      } else {
        orderedCursor(
          indexOrder,
          indexQueries.map(query =>
            state.query.relationshipIndexSeek(
              index,
              needsValues = needsValues || indexOrder != IndexOrderNone,
              indexOrder,
              query,
              includeChangesFromThisTransaction
            )
          ).toArray
        )
      }
    case LockingUniqueIndexSeek =>
      val indexQueries = computeExactQueries(state, baseContext)
      if (indexQueries.size == 1) {
        state.query.relationshipLockingUniqueIndexSeek(index, indexQueries.head)
      } else {
        orderedCursor(
          indexOrder,
          indexQueries.map(query =>
            state.query.relationshipLockingUniqueIndexSeek(
              index,
              query
            )
          ).toArray
        )
      }
  }

  private def orderedCursor(indexOrder: IndexOrder, cursors: Array[NodeValueIndexCursor]) = indexOrder match {
    case IndexOrderNone       => CompositeValueIndexCursor.unordered(cursors)
    case IndexOrderAscending  => CompositeValueIndexCursor.ascending(cursors)
    case IndexOrderDescending => CompositeValueIndexCursor.descending(cursors)
  }

  private def orderedCursor(indexOrder: IndexOrder, cursors: Array[RelationshipValueIndexCursor]) = indexOrder match {
    case IndexOrderNone       => CompositeValueIndexCursor.unordered(cursors)
    case IndexOrderAscending  => CompositeValueIndexCursor.ascending(cursors)
    case IndexOrderDescending => CompositeValueIndexCursor.descending(cursors)
  }

  private lazy val compiled: CompiledQueryExpression[Expression] =
    QueryExpressionSupport.compile[Expression, Expression](
      valueExpr,
      propertyIds,
      identity[Expression],
      RuntimeRangeExtractor
    )

  def computeIndexQueries(state: QueryState, row: ReadableRow): collection.Seq[Seq[PropertyIndexQuery]] = {
    val resolver = new ValueResolver[Expression] {
      override def eval(prepared: Expression): AnyValue = prepared(row, state)
    }
    compiled.apply(resolver)
  }

  private def computeExactQueries(
    state: QueryState,
    row: ReadableRow
  ): collection.Seq[Seq[PropertyIndexQuery.ExactPredicate]] = {
    val all = computeIndexQueries(state, row)
    if (!all.forall(_.forall(_.isInstanceOf[PropertyIndexQuery.ExactPredicate]))) {
      throw InternalException.internalError(
        this.getClass.getSimpleName,
        "Expected only exact for LockingUniqueIndexSeek"
      )
    }
    all.asInstanceOf[collection.Seq[Seq[PropertyIndexQuery.ExactPredicate]]]
  }
}
