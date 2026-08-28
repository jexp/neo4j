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
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.ClosingLongIterator
import org.neo4j.cypher.internal.runtime.ClosingRelationshipIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.PrimitiveLongHelper
import org.neo4j.cypher.internal.runtime.interpreted.pipes.DirectedUnionRelationshipTypesScanPipe.unionTypeIterator
import org.neo4j.cypher.internal.runtime.iterators.BaseRelationshipCursorIterator
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor
import org.neo4j.internal.kernel.api.TokenReadSession
import org.neo4j.internal.kernel.api.helpers.UnionRelationshipTypeIndexCursor
import org.neo4j.internal.kernel.api.helpers.UnionRelationshipTypeIndexCursor.ascendingUnionRelationshipTypeIndexCursor
import org.neo4j.internal.kernel.api.helpers.UnionRelationshipTypeIndexCursor.descendingUnionRelationshipTypeIndexCursor
import org.neo4j.io.IOUtils
import org.neo4j.kernel.api.StatementConstants

case class DirectedUnionRelationshipTypesScanPipe(
  ident: Option[String],
  fromNode: Option[String],
  types: Seq[LazyTypeStatic],
  toNode: Option[String],
  indexOrder: IndexOrder,
  includeChangesFromThisTransaction: Boolean
)(
  val id: Id =
    Id.INVALID_ID
) extends Pipe {

  private val relationshipWriter = Relationships.compileRelationshipWriter(ident, fromNode, toNode)

  protected def internalCreateResults(state: QueryState): ClosingIterator[CypherRow] = {
    val relIterator = unionTypeIterator(
      state,
      types,
      indexOrder,
      state.relTypeTokenReadSession.get,
      callReadFromStore = fromNode.nonEmpty || toNode.nonEmpty,
      includeChangesFromThisTransaction
    )
    val ctx = state.newRowWithArgument(rowFactory)
    PrimitiveLongHelper.map(
      relIterator,
      relationshipId => {
        relationshipWriter.writeRow(
          rowFactory,
          ctx,
          relationshipId,
          relIterator
        )
      }
    )
  }
}

object DirectedUnionRelationshipTypesScanPipe {

  def unionTypeIterator(
    state: QueryState,
    types: Seq[LazyTypeStatic],
    indexOrder: IndexOrder,
    tokenReadSession: TokenReadSession,
    callReadFromStore: Boolean,
    includeChangesFromThisTransaction: Boolean
  ): ClosingRelationshipIterator = {
    val ids = types.map(_.getId(state.query)).filter(_ != LazyType.UNKNOWN).toArray
    if (ids.isEmpty) {
      ClosingLongIterator.emptyClosingRelationshipIterator
    } else {
      unionTypeIterator(state, ids, indexOrder, tokenReadSession, callReadFromStore, includeChangesFromThisTransaction)
    }
  }

  def unionTypeIterator(
    state: QueryState,
    types: Array[Int],
    indexOrder: IndexOrder,
    tokenReadSession: TokenReadSession,
    callReadFromStore: Boolean,
    includeChangesFromThisTransaction: Boolean
  ): ClosingRelationshipIterator = {
    val query = state.query
    val cursors = types.map(_ => {
      val c = query.relationshipTypeIndexCursor()
      query.resources.trace(c)
      c
    })
    val read = query.transactionalContext.dataRead
    val cursor = indexOrder match {
      case IndexOrderAscending | IndexOrderNone =>
        ascendingUnionRelationshipTypeIndexCursor(
          read,
          tokenReadSession,
          query.transactionalContext.cursorContext,
          types,
          cursors,
          includeChangesFromThisTransaction
        )
      case IndexOrderDescending => descendingUnionRelationshipTypeIndexCursor(
          read,
          tokenReadSession,
          query.transactionalContext.cursorContext,
          types,
          cursors,
          includeChangesFromThisTransaction
        )
    }

    if (callReadFromStore) {
      storeAccessingIterator(cursor, cursors)
    } else {
      nonStoreAccessingIterator(cursor, cursors)
    }
  }

  private def storeAccessingIterator(
    cursor: UnionRelationshipTypeIndexCursor,
    cursors: Array[RelationshipTypeIndexCursor]
  ): ClosingRelationshipIterator = {
    new BaseUnionTypeIterator(cursors) {

      override protected def fetchNext(): Long = {
        while (cursor.next() && cursor.readFromStore()) {
          return cursor.reference()
        }
        StatementConstants.NO_SUCH_RELATIONSHIP
      }

      /**
       * Store the current state in case the underlying cursor is closed when calling next.
       */
      final override protected def storeState(): Unit = {
        relTypeId = cursor.`type`()
        source = cursor.sourceNodeReference()
        target = cursor.targetNodeReference()
      }
    }
  }

  private def nonStoreAccessingIterator(
    cursor: UnionRelationshipTypeIndexCursor,
    cursors: Array[RelationshipTypeIndexCursor]
  ): ClosingRelationshipIterator = {
    new BaseUnionTypeIterator(cursors) {

      /**
       * Store the current state in case the underlying cursor is closed when calling next.
       */
      final override protected def storeState(): Unit = {
        relTypeId = cursor.`type`()
      }
      override protected def fetchNext(): Long = {
        if (cursor.next()) {
          cursor.reference()
        } else {
          StatementConstants.NO_SUCH_RELATIONSHIP
        }
      }
    }
  }

  abstract private class BaseUnionTypeIterator(
    cursors: Array[RelationshipTypeIndexCursor]
  ) extends BaseRelationshipCursorIterator {

    final override def close(): Unit = IOUtils.closeAll(cursors: _*)
  }
}
