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

import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.PrimitiveLongHelper
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.DirectedAllRelationshipsScanPipe.allRelationshipsIterator
import org.neo4j.cypher.internal.runtime.iterators.BaseRelationshipCursorIterator
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.values.virtual.VirtualValues

case class DirectedAllRelationshipsScanPipe(
  ident: Option[String],
  fromNode: Option[String],
  toNode: Option[String],
  includeChangesFromThisTransaction: Boolean
)(val id: Id = Id.INVALID_ID) extends Pipe {

  private val relationshipWriter = Relationships.compileRelationshipWriter(ident, fromNode, toNode)

  protected def internalCreateResults(state: QueryState): ClosingIterator[CypherRow] = {
    val ctx = state.newRowWithArgument(rowFactory)
    val query: QueryContext = state.query
    val relIterator = allRelationshipsIterator(query, includeChangesFromThisTransaction)
    PrimitiveLongHelper.map(
      relIterator,
      r => {
        relationshipWriter.writeRow(
          rowFactory,
          ctx,
          VirtualValues.relationship(r),
          VirtualValues.node(relIterator.startNodeId()),
          VirtualValues.node(relIterator.endNodeId())
        )
      }
    )
  }
}

object DirectedAllRelationshipsScanPipe {

  def allRelationshipsIterator(
    query: QueryContext,
    includeChangesFromThisTransaction: Boolean
  ): BaseRelationshipCursorIterator = {
    val read = query.transactionalContext.dataRead
    val cursor = query.scanCursor()
    query.resources.trace(cursor)
    read.allRelationshipsScan(cursor, includeChangesFromThisTransaction)
    new BaseRelationshipCursorIterator {
      override protected def fetchNext(): Long = if (cursor.next()) cursor.reference() else -1L
      override def close(): Unit = cursor.close()
      override protected def storeState(): Unit = {
        relTypeId = cursor.`type`()
        source = cursor.sourceNodeReference()
        target = cursor.targetNodeReference()
      }
    }
  }
}
