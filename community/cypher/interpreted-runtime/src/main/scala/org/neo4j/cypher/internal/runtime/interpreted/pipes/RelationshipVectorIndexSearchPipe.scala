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

import org.neo4j.cypher.internal.logical.plans.EntityFilterQueryExpression
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.QueryExpression
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.ClosingLongIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.PrimitiveLongHelper
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.RelationshipIterator
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.NodeVectorIndexSearchPipe.predicate
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RelationshipVectorIndexSearchPipe.vectorSearchCursor
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.operations.CypherCoercions.validateAndConvertVectorIndexQuery
import org.neo4j.cypher.operations.CypherFunctions
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor
import org.neo4j.storageengine.api.RelationshipVisitor
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.NO_VALUE

abstract class RelationshipVectorIndexSearchPipe(
  properties: Array[Int],
  vectorExpression: Expression,
  limitExpression: Expression,
  queryIndexId: Int,
  entityFilterExpression: EntityFilterQueryExpression[Expression],
  propertyFilterExpression: Option[QueryExpression[Expression]]
) extends Pipe {

  protected def newRow(
    row: CypherRow,
    iterator: RelationshipVectorSearchIterator
  ): CypherRow

  protected def iteratorFrom(cursor: RelationshipValueIndexCursor): RelationshipVectorSearchIterator

  protected def internalCreateResults(
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    val incomingRow = state.newRowWithArgument(rowFactory)
    val index = state.queryIndexes(queryIndexId)
    val vector = vectorExpression(incomingRow, state)
    if (vector eq NO_VALUE) {
      ClosingIterator.empty
    } else {
      val cursor = vectorSearchCursor(
        state.query,
        index,
        limitExpression,
        vector,
        entityFilterExpression,
        propertyFilterExpression,
        properties,
        incomingRow,
        state
      )

      val iterator = iteratorFrom(cursor)
      PrimitiveLongHelper.map(
        iterator,
        _ => newRow(state.newRowWithArgument(rowFactory), iterator)
      )
    }
  }

  class RelationshipVectorSearchIterator(cursor: RelationshipValueIndexCursor) extends ClosingLongIterator
      with RelationshipIterator {
    private[this] var hasFetchedNext = false
    private[this] var exhausted = false

    override def typeId: Int = cursor.`type`()

    def score: Float = cursor.score()

    def reference: Long = cursor.reference()

    override def startNodeId(): Long = cursor.sourceNodeReference()

    override def endNodeId(): Long = cursor.targetNodeReference()

    override protected[this] def innerHasNext: Boolean = {
      if (!hasFetchedNext) {
        hasFetchedNext = true
        if (!fetchNext()) {
          exhausted = true
        }
      }
      !exhausted
    }

    override def next(): Long = {
      if (!hasNext) {
        close()
        throw new NoSuchElementException
      }
      hasFetchedNext = false
      cursor.reference()
    }

    override def close(): Unit = {
      cursor.close()
    }

    protected[this] def fetchNext(): Boolean = {
      while (cursor.next() && cursor.readFromStore()) {
        return true
      }
      false
    }

    override def relationshipVisit[EXCEPTION <: Exception](
      relationshipId: Long,
      visitor: RelationshipVisitor[EXCEPTION]
    ): Boolean = {
      visitor.visit(reference, typeId, startNodeId(), endNodeId())
      true
    }
  }

  class NonStoreAccessingVectorSearchIterator(cursor: RelationshipValueIndexCursor)
      extends RelationshipVectorSearchIterator(cursor) {
    final override protected[this] def fetchNext(): Boolean = cursor.next()
  }

  class UndirectedRelationshipVectorSearchIterator(cursor: RelationshipValueIndexCursor)
      extends RelationshipVectorSearchIterator(cursor) {
    private[this] var emitSibling: Boolean = false

    final override protected[this] def fetchNext(): Boolean = {
      if (emitSibling) {
        emitSibling = false
        true
      } else {
        val next = super.fetchNext()
        if (next) {
          emitSibling = cursor.sourceNodeReference() != cursor.targetNodeReference()
        }
        next
      }
    }

    override def startNodeId(): Long = {
      if (emitSibling) {
        cursor.sourceNodeReference()
      } else {
        cursor.targetNodeReference()
      }
    }

    override def endNodeId(): Long = {
      if (emitSibling) {
        cursor.targetNodeReference()
      } else {
        cursor.sourceNodeReference()
      }
    }
  }
}

abstract class BaseRelationshipVectorIndexSearchPipe(
  ident: Option[String],
  fromNode: Option[String],
  toNode: Option[String],
  score: Option[String],
  properties: Array[Int],
  vectorExpression: Expression,
  limitExpression: Expression,
  queryIndexId: Int,
  entityFilterExpression: EntityFilterQueryExpression[Expression],
  propertyFilterExpression: Option[QueryExpression[Expression]]
) extends RelationshipVectorIndexSearchPipe(
      properties,
      vectorExpression,
      limitExpression,
      queryIndexId,
      entityFilterExpression,
      propertyFilterExpression
    ) {

  private val relationshipWriter: Relationships.RelationshipWriter =
    Relationships.compileRelationshipWriter(ident, fromNode, toNode)

  private[this] val _newRow: (CypherRow, RelationshipVectorSearchIterator) => CypherRow = {
    score match {
      case Some(value) =>
        (incomingRow: CypherRow, iterator: RelationshipVectorSearchIterator) =>
          val row = relationshipWriter.writeRow(
            rowFactory,
            incomingRow,
            iterator.reference,
            iterator
          )
          row.set(value, Values.floatValue(iterator.score))
          row
      case None => (incomingRow: CypherRow, iterator: RelationshipVectorSearchIterator) =>
          relationshipWriter.writeRow(
            rowFactory,
            incomingRow,
            iterator.reference,
            iterator
          )
    }
  }

  override protected def newRow(
    row: CypherRow,
    iterator: RelationshipVectorSearchIterator
  ): CypherRow = _newRow(row, iterator)
}

case class DirectedRelationshipVectorIndexSearchPipe(
  ident: Option[String],
  fromNode: Option[String],
  toNode: Option[String],
  score: Option[String],
  properties: Array[Int],
  vectorExpression: Expression,
  limitExpression: Expression,
  queryIndexId: Int,
  entityFilterExpression: EntityFilterQueryExpression[Expression],
  propertyFilterExpression: Option[QueryExpression[Expression]]
)(val id: Id = Id.INVALID_ID)
    extends BaseRelationshipVectorIndexSearchPipe(
      ident,
      fromNode,
      toNode,
      score,
      properties,
      vectorExpression,
      limitExpression,
      queryIndexId,
      entityFilterExpression,
      propertyFilterExpression
    ) {

  override protected def iteratorFrom(cursor: RelationshipValueIndexCursor): RelationshipVectorSearchIterator = {
    if (fromNode.nonEmpty || toNode.nonEmpty) {
      new RelationshipVectorSearchIterator(cursor)
    } else {
      new NonStoreAccessingVectorSearchIterator(cursor)
    }
  }
}

case class UndirectedRelationshipVectorIndexSearchPipe(
  ident: Option[String],
  fromNode: Option[String],
  toNode: Option[String],
  score: Option[String],
  properties: Array[Int],
  vectorExpression: Expression,
  limitExpression: Expression,
  queryIndexId: Int,
  entityFilterExpression: EntityFilterQueryExpression[Expression],
  propertyFilterExpression: Option[QueryExpression[Expression]]
)(val id: Id = Id.INVALID_ID)
    extends BaseRelationshipVectorIndexSearchPipe(
      ident,
      fromNode,
      toNode,
      score,
      properties,
      vectorExpression,
      limitExpression,
      queryIndexId,
      entityFilterExpression,
      propertyFilterExpression
    ) {

  override protected def iteratorFrom(cursor: RelationshipValueIndexCursor): RelationshipVectorSearchIterator =
    new UndirectedRelationshipVectorSearchIterator(cursor)
}

object RelationshipVectorIndexSearchPipe {

  def vectorSearchCursor(
    query: QueryContext,
    index: IndexReadSession,
    limit: Expression,
    vector: AnyValue,
    entityFilter: EntityFilterQueryExpression[Expression],
    filter: Option[QueryExpression[Expression]],
    properties: Array[Int],
    row: CypherRow,
    state: QueryState
  ): RelationshipValueIndexCursor = {
    val l = CypherFunctions.asNonNegativeIntExact(limit(row, state))
    if (l == 0) {
      RelationshipValueIndexCursor.EMPTY
    } else {
      val queries =
        predicate(
          l,
          validateAndConvertVectorIndexQuery(index.reference(), vector),
          entityFilter,
          filter,
          properties,
          row,
          state
        )
      if (queries.nonEmpty) {
        // No includeChangesFromThisTransaction: a vector reader never reads transaction state.
        query.relationshipIndexSeek(
          index,
          needsValues = false,
          IndexOrderNone,
          queries
        )
      } else {
        RelationshipValueIndexCursor.EMPTY
      }
    }
  }

}
