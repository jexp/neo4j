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

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.neo4j.cypher.internal.planner.spi.CardinalityByLabelsAndRelationshipType
import org.neo4j.cypher.internal.planner.spi.GraphStatisticsSnapshot
import org.neo4j.cypher.internal.planner.spi.NamedCount
import org.neo4j.cypher.internal.planner.spi.NodesAllCardinality

import java.util

import scala.jdk.CollectionConverters.SeqHasAsJava

class GraphStatisticsSnapshotBuilderTest {

  @Test
  def emptyGraphHasNoEntries(): Unit = {
    val snapshot = GraphStatisticsSnapshotBuilder.from(entries(), entries())

    assertThat(snapshot.statsValues.size).isEqualTo(0)
  }

  @Test
  def totalsReuseThePlannersOwnNoFilterKeys(): Unit = {
    val snapshot = GraphStatisticsSnapshotBuilder.from(
      entries(entry("count" -> Long.box(42L))),
      entries(entry("count" -> Long.box(7L)))
    )

    assertThat(snapshot.statsValues(NodesAllCardinality)).isEqualTo(42.0)
    assertThat(snapshot.statsValues(CardinalityByLabelsAndRelationshipType(None, None, None))).isEqualTo(7.0)
  }

  @Test
  def singleLabelIsKeyedByItsCypherPattern(): Unit = {
    val snapshot = GraphStatisticsSnapshotBuilder.from(
      entries(
        entry("count" -> Long.box(10L)),
        entry("label" -> "Person", "count" -> Long.box(3L))
      ),
      entries()
    )

    assertThat(snapshot.statsValues(NamedCount("(:Person)"))).isEqualTo(3.0)
  }

  @Test
  def multipleLabelsAndRelationshipTypesAreAllRepresented(): Unit = {
    val snapshot = GraphStatisticsSnapshotBuilder.from(
      entries(
        entry("count" -> Long.box(10L)),
        entry("label" -> "Person", "count" -> Long.box(3L)),
        entry("label" -> "Company", "count" -> Long.box(7L))
      ),
      entries(
        entry("count" -> Long.box(20L)),
        entry("relationshipType" -> "KNOWS", "count" -> Long.box(5L)),
        entry("relationshipType" -> "WORKS_FOR", "count" -> Long.box(15L))
      )
    )

    val namedKeys = snapshot.statsValues.keySet.collect { case NamedCount(key) => key }.toSeq.asJava
    assertThat(namedKeys).containsExactlyInAnyOrder(
      "(:Person)",
      "(:Company)",
      "()-[:KNOWS]->()",
      "()-[:WORKS_FOR]->()"
    )
    assertThat(snapshot.statsValues(NodesAllCardinality)).isEqualTo(10.0)
    assertThat(snapshot.statsValues(CardinalityByLabelsAndRelationshipType(None, None, None))).isEqualTo(20.0)
    assertThat(snapshot.statsValues(NamedCount("(:Person)"))).isEqualTo(3.0)
    assertThat(snapshot.statsValues(NamedCount("(:Company)"))).isEqualTo(7.0)
    assertThat(snapshot.statsValues(NamedCount("()-[:KNOWS]->()"))).isEqualTo(5.0)
    assertThat(snapshot.statsValues(NamedCount("()-[:WORKS_FOR]->()"))).isEqualTo(15.0)
  }

  @Test
  def startAndEndLabelBreakdownEntriesAreDistinctKeys(): Unit = {
    val snapshot = GraphStatisticsSnapshotBuilder.from(
      entries(),
      entries(
        entry("relationshipType" -> "KNOWS", "count" -> Long.box(5L)),
        entry("relationshipType" -> "KNOWS", "startLabel" -> "Person", "count" -> Long.box(2L)),
        entry("relationshipType" -> "KNOWS", "endLabel" -> "Person", "count" -> Long.box(4L))
      )
    )

    assertThat(snapshot.statsValues(NamedCount("()-[:KNOWS]->()"))).isEqualTo(5.0)
    assertThat(snapshot.statsValues(NamedCount("(:Person)-[:KNOWS]->()"))).isEqualTo(2.0)
    assertThat(snapshot.statsValues(NamedCount("()-[:KNOWS]->(:Person)"))).isEqualTo(4.0)
  }

  @Test
  def twoIndependentlyBuiltSnapshotsFromIdenticalInputAreEqual(): Unit = {
    def build(): GraphStatisticsSnapshot = GraphStatisticsSnapshotBuilder.from(
      entries(
        entry("count" -> Long.box(10L)),
        entry("label" -> "Person", "count" -> Long.box(3L))
      ),
      entries(
        entry("count" -> Long.box(20L)),
        entry("relationshipType" -> "KNOWS", "startLabel" -> "Person", "count" -> Long.box(2L))
      )
    )

    assertThat(build()).isEqualTo(build())
  }

  @Test
  def namesContainingPatternSyntaxAreBacktickQuotedAndCannotCollide(): Unit = {
    // Without quoting, a single-type entry whose name embeds "()-[:Z]->(" would render to the
    // exact same raw string as a differently-shaped (type, startLabel) breakdown entry -- e.g.
    // type "X)-[:Y]->(" alone, and type "Y]->()-[:Z" with start label "X", both naively render to
    // "(:X)-[:Y]->()-[:Z]->()". Backtick-quoting each name keeps them distinguishable.
    val snapshot = GraphStatisticsSnapshotBuilder.from(
      entries(),
      entries(
        entry("relationshipType" -> "X)-[:Y]->(", "count" -> Long.box(1L)),
        entry("relationshipType" -> "Y]->()-[:Z", "startLabel" -> "X", "count" -> Long.box(2L))
      )
    )

    assertThat(snapshot.statsValues.size).isEqualTo(2)
    assertThat(snapshot.statsValues(NamedCount("()-[:`X)-[:Y]->(`]->()"))).isEqualTo(1.0)
    assertThat(snapshot.statsValues(NamedCount("(:X)-[:`Y]->()-[:Z`]->()"))).isEqualTo(2.0)
  }

  private def entries(values: util.Map[String, AnyRef]*): util.List[util.Map[String, AnyRef]] =
    values.asJava

  private def entry(pairs: (String, AnyRef)*): util.Map[String, AnyRef] = {
    val map = new util.HashMap[String, AnyRef]()
    pairs.foreach { case (k, v) => map.put(k, v) }
    map
  }
}
