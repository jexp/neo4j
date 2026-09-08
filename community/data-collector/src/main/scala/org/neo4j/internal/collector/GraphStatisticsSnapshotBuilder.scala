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
package org.neo4j.internal.collector

import org.neo4j.cypher.internal.planner.spi.CardinalityByLabelsAndRelationshipType
import org.neo4j.cypher.internal.planner.spi.GraphStatisticsSnapshot
import org.neo4j.cypher.internal.planner.spi.NamedCount
import org.neo4j.cypher.internal.planner.spi.NodesAllCardinality
import org.neo4j.cypher.internal.planner.spi.StatisticsKey
import org.neo4j.util.Stringifier

import java.util

import scala.jdk.CollectionConverters.ListHasAsScala

/**
 * Flattens [[GraphCountsSection]]'s counts into a [[GraphStatisticsSnapshot]] for staleness
 * comparison. Names are backtick-quoted via `Stringifier.backtick` so differently-shaped entries
 * can never collide on the same key string.
 */
object GraphStatisticsSnapshotBuilder {

  def from(
    nodeCounts: util.List[util.Map[String, AnyRef]],
    relationshipCounts: util.List[util.Map[String, AnyRef]]
  ): GraphStatisticsSnapshot = {
    val nodeEntries = nodeCounts.asScala.map(entry => nodeKey(entry) -> count(entry))
    val relationshipEntries = relationshipCounts.asScala.map(entry => relationshipKey(entry) -> count(entry))
    GraphStatisticsSnapshot((nodeEntries ++ relationshipEntries).toMap)
  }

  private def nodeKey(entry: util.Map[String, AnyRef]): StatisticsKey =
    stringValue(entry, "label") match {
      case Some(label) => NamedCount(s"(:${Stringifier.backtick(label)})")
      case None        => NodesAllCardinality
    }

  private def relationshipKey(entry: util.Map[String, AnyRef]): StatisticsKey = {
    val startLabel = stringValue(entry, "startLabel")
    val relType = stringValue(entry, "relationshipType")
    val endLabel = stringValue(entry, "endLabel")
    (startLabel, relType, endLabel) match {
      case (None, None, None) => CardinalityByLabelsAndRelationshipType(None, None, None)
      case (None, Some(t), None) =>
        NamedCount(s"()-[:${Stringifier.backtick(t)}]->()")
      case (Some(s), Some(t), None) =>
        NamedCount(s"(:${Stringifier.backtick(s)})-[:${Stringifier.backtick(t)}]->()")
      case (None, Some(t), Some(e)) =>
        NamedCount(s"()-[:${Stringifier.backtick(t)}]->(:${Stringifier.backtick(e)})")
      case _ => throw new IllegalArgumentException("Unexpected relationship count entry: " + entry)
    }
  }

  private def stringValue(entry: util.Map[String, AnyRef], field: String): Option[String] =
    Option(entry.get(field)).map(_.toString)

  private def count(entry: util.Map[String, AnyRef]): Double =
    entry.get("count").asInstanceOf[Number].doubleValue()
}
