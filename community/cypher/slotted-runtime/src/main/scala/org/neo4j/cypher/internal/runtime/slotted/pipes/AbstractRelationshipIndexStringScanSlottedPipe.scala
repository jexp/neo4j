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

import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlottedIndexedProperty
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.IndexIteratorBase
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor
import org.neo4j.values.storable.TextValue

abstract class AbstractRelationshipIndexStringScanSlottedPipe(
  offset: Option[Int],
  startNode: Option[Int],
  endNode: Option[Int],
  property: SlottedIndexedProperty,
  queryIndexId: Int,
  valueExpr: Expression
) extends Pipe with IndexSlottedPipeWithValues {

  override val indexPropertySlotOffsets: Array[Int] = property.maybeCachedEntityPropertySlot.toArray

  override val indexPropertyIndices: Array[Int] =
    if (property.maybeCachedEntityPropertySlot.isDefined) Array(0) else Array.empty
  protected val needsValues: Boolean = indexPropertyIndices.nonEmpty

  override protected def internalCreateResults(state: QueryState): ClosingIterator[CypherRow] = {
    val baseContext = state.newRowWithArgument(rowFactory)
    val value = valueExpr(baseContext, state)

    val resultNodes = value match {
      case value: TextValue =>
        iterator(
          state,
          startNode,
          endNode,
          baseContext,
          queryContextCall(state, state.queryIndexes(queryIndexId), value)
        )
      case _ =>
        ClosingIterator.empty
    }

    resultNodes
  }

  protected def queryContextCall(
    state: QueryState,
    index: IndexReadSession,
    value: TextValue
  ): RelationshipValueIndexCursor

  protected def iterator(
    state: QueryState,
    startOffset: Option[Int],
    endOffset: Option[Int],
    baseContext: CypherRow,
    cursor: RelationshipValueIndexCursor
  ): IndexIteratorBase[CypherRow]
}

trait Directed {
  self: AbstractRelationshipIndexStringScanSlottedPipe =>

  override protected def iterator(
    state: QueryState,
    startOffset: Option[Int],
    endOffset: Option[Int],
    baseContext: CypherRow,
    cursor: RelationshipValueIndexCursor
  ): IndexIteratorBase[CypherRow] =
    new SlottedRelationshipIndexIterator(state, startOffset, endOffset, cursor)
}

trait Undirected {
  self: AbstractRelationshipIndexStringScanSlottedPipe =>

  override protected def iterator(
    state: QueryState,
    startOffset: Option[Int],
    endOffset: Option[Int],
    baseContext: CypherRow,
    cursor: RelationshipValueIndexCursor
  ): IndexIteratorBase[CypherRow] =
    new SlottedUndirectedRelationshipIndexIterator(state, startOffset, endOffset, cursor)
}

case class DirectedRelationshipIndexContainsScanSlottedPipe(
  offset: Option[Int],
  startNode: Option[Int],
  endNode: Option[Int],
  property: SlottedIndexedProperty,
  queryIndexId: Int,
  valueExpr: Expression,
  slots: SlotConfiguration,
  indexOrder: IndexOrder,
  includeChangesFromThisTransaction: Boolean
)(val id: Id = Id.INVALID_ID)
    extends AbstractRelationshipIndexStringScanSlottedPipe(
      offset,
      startNode,
      endNode,
      property,
      queryIndexId,
      valueExpr
    ) with Directed {

  override protected def queryContextCall(
    state: QueryState,
    index: IndexReadSession,
    value: TextValue
  ): RelationshipValueIndexCursor =
    state.query.relationshipIndexSeekByContains(
      index,
      needsValues,
      indexOrder,
      value,
      includeChangesFromThisTransaction
    )
}

case class UndirectedRelationshipIndexContainsScanSlottedPipe(
  offset: Option[Int],
  startNode: Option[Int],
  endNode: Option[Int],
  property: SlottedIndexedProperty,
  queryIndexId: Int,
  valueExpr: Expression,
  slots: SlotConfiguration,
  indexOrder: IndexOrder,
  includeChangesFromThisTransaction: Boolean
)(val id: Id = Id.INVALID_ID)
    extends AbstractRelationshipIndexStringScanSlottedPipe(
      offset,
      startNode,
      endNode,
      property,
      queryIndexId,
      valueExpr
    ) with Undirected {

  override protected def queryContextCall(
    state: QueryState,
    index: IndexReadSession,
    value: TextValue
  ): RelationshipValueIndexCursor =
    state.query.relationshipIndexSeekByContains(
      index,
      needsValues,
      indexOrder,
      value,
      includeChangesFromThisTransaction
    )
}

case class DirectedRelationshipIndexEndsWithScanSlottedPipe(
  offset: Option[Int],
  startNode: Option[Int],
  endNode: Option[Int],
  property: SlottedIndexedProperty,
  queryIndexId: Int,
  valueExpr: Expression,
  slots: SlotConfiguration,
  indexOrder: IndexOrder,
  includeChangesFromThisTransaction: Boolean
)(val id: Id = Id.INVALID_ID)
    extends AbstractRelationshipIndexStringScanSlottedPipe(
      offset,
      startNode,
      endNode,
      property,
      queryIndexId,
      valueExpr
    ) with Directed {

  override protected def queryContextCall(
    state: QueryState,
    index: IndexReadSession,
    value: TextValue
  ): RelationshipValueIndexCursor =
    state.query.relationshipIndexSeekByEndsWith(
      index,
      needsValues,
      indexOrder,
      value,
      includeChangesFromThisTransaction
    )
}

case class UndirectedRelationshipIndexEndsWithScanSlottedPipe(
  offset: Option[Int],
  startNode: Option[Int],
  endNode: Option[Int],
  property: SlottedIndexedProperty,
  queryIndexId: Int,
  valueExpr: Expression,
  slots: SlotConfiguration,
  indexOrder: IndexOrder,
  includeChangesFromThisTransaction: Boolean
)(val id: Id = Id.INVALID_ID)
    extends AbstractRelationshipIndexStringScanSlottedPipe(
      offset,
      startNode,
      endNode,
      property,
      queryIndexId,
      valueExpr
    ) with Undirected {

  override protected def queryContextCall(
    state: QueryState,
    index: IndexReadSession,
    value: TextValue
  ): RelationshipValueIndexCursor =
    state.query.relationshipIndexSeekByEndsWith(
      index,
      needsValues,
      indexOrder,
      value,
      includeChangesFromThisTransaction
    )
}
