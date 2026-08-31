/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.util.test_helpers

import org.neo4j.cypher.internal.util.test_helpers.InMemoryGraph.Rel
import org.neo4j.cypher.internal.util.test_helpers.graphtemplate.NamedEntites
import org.neo4j.cypher.internal.util.test_helpers.graphtemplate.TemplateInstantiator
import org.neo4j.cypher.internal.util.test_helpers.graphtemplate.parsing.GraphTemplateParser
import org.neo4j.storageengine.api.RelationshipDirection

import java.util.function.LongPredicate

import scala.collection.mutable

class InMemoryGraph(val nodes: Map[Long, Set[Long]], val rels: Map[Long, InMemoryGraph.Rel]) {
  def lastId: Long = nodes.size + rels.size - 1
  def nextId: Long = lastId + 1

  def rel(id: Long): Option[Rel] = rels.get(id)

  def labels(id: Long): Set[Long] = nodes.getOrElse(id, Set.empty)

  def nodeRels(node: Long): Iterator[(Rel, RelationshipDirection)] =
    rels.values.iterator.collect {
      case rel if node == rel.source && node == rel.target =>
        rel -> RelationshipDirection.LOOP
      case rel if node == rel.source =>
        rel -> RelationshipDirection.OUTGOING
      case rel if node == rel.target =>
        rel -> RelationshipDirection.INCOMING
    }
}

object InMemoryGraph {

  case class Rel(id: Long, source: Long, target: Long, relType: Int) {
    override def toString: String = s"($source)-[$id:$relType]->($target)"
  }

  val empty: InMemoryGraph = new InMemoryGraph(Map.empty, Map.empty)
  val DEFAULT_REL = 1

  def builder = new Builder

  class Builder {
    private val nodes = mutable.Map.empty[Long, Set[Long]]
    private val rels = mutable.Map.empty[Long, Rel]

    def node(labels: Long*): Long = {
      val nextId: Long = nodes.size + rels.size
      nodes += (nextId -> labels.toSet)
      nextId
    }

    def addLabel(id: Long, labelId: Long): Unit =
      nodes(id) += labelId

    def rel(source: Long, target: Long): Long = rel(source, target, DEFAULT_REL)

    def rel(source: Long, target: Long, relType: Int): Long = {
      val nextId: Long = nodes.size + rels.size
      rels += nextId -> Rel(nextId, source, target, relType)
      nextId
    }

    def line(length: Int): Seq[Long] = {
      val nodes = (0 to length).map(_ => node())
      nodes.zip(nodes.drop(1)).foreach { case (a, b) => rel(a, b) }
      nodes
    }

    def chainRel(source: Long, target: Long, length: Int): Unit = {
      var current = source
      for (_ <- 1 until length) {
        val next = node()
        rel(current, next)
        current = next
      }
      rel(current, target)
    }

    def build(): InMemoryGraph = new InMemoryGraph(nodes.toMap, rels.toMap)
  }

  def fromTemplate(template: String): Templated = {
    val builder = new StringMapped.Builder
    val entities = GraphTemplateParser.parse(template).instantiate(builder)
    val graph = builder.build()
    new Templated(graph, entities)
  }

  /** StringMapped graph with mapping of names of entities in a graph template string */
  class Templated(
    graph: StringMapped,
    entities: NamedEntites[Long, Long]
  ) extends StringMapped(graph.graph, graph.relTypes, graph.labels) {
    infix def node(name: String): Long = entities.node(name)
    infix def rel(name: String): Long = entities.rel(name)
  }

  /** Provides mappings from String labels and rel types to their primitive ids */
  class StringMapped(
    val graph: InMemoryGraph,
    val relTypes: Map[String, Int],
    val labels: Map[String, Long]
  ) extends InMemoryGraph(graph.nodes, graph.rels) {

    def hasLabel(label: String): LongPredicate = {
      labels.get(label) match {
        case Some(value) => node => labels(node).contains(value)
        case None        => _ => false
      }
    }

    def nodesWithLabel(name: String): Iterable[Long] = {
      val labelId = labels(name)
      graph.nodes.filter { case (_, labels) => labels.contains(labelId) }.keys
    }
  }

  object StringMapped {

    class Builder extends TemplateInstantiator[Long, Long] {
      private val builder = InMemoryGraph.builder
      private val relTypeMap = mutable.Map.empty[String, Int]
      private val labelMap = mutable.Map.empty[String, Long]

      def createNode(labels: Seq[String]): Long = {
        val labelIds = labels.map { name => labelMap.getOrElseUpdate(name, labelMap.size + 1) }
        builder.node(labelIds: _*)
      }

      def addLabel(id: Long, label: String): Unit = {
        val labelId = labelMap.getOrElseUpdate(label, labelMap.size + 1)
        builder.addLabel(id, labelId)
      }

      def createRel(from: Long, to: Long, relType: Option[String]): Long = {
        // the default relType is 1, custom relTypes begin at 2
        val relTypeId =
          relType.map(name => relTypeMap.getOrElseUpdate(name, relTypeMap.size + 2)).getOrElse(DEFAULT_REL)
        builder.rel(from, to, relTypeId)
      }

      def build(): InMemoryGraph.StringMapped = {
        val graph = builder.build()

        new StringMapped(
          graph,
          relTypeMap.toMap,
          labelMap.toMap
        )
      }
    }

    class GraphOps(builder: StringMapped.Builder) extends GraphOperations {
      override type Node = StringMappedNode

      class StringMappedNode(val id: Long) extends GraphOperations.Node[StringMappedNode] {

        override def createRelationshipTo(other: StringMappedNode, relType: String): Unit = {
          builder.createRel(id, other.id, Some(relType))
        }

        override def addLabel(label: String): Unit =
          builder.addLabel(id, label)
      }

      override def createNode(labels: String*): StringMappedNode = {
        new StringMappedNode(builder.createNode(labels))
      }
    }
  }
}
