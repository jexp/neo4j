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
package org.neo4j.cypher.internal.util.test_helpers.graphtemplate

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.collection.mutable.ArrayBuffer

case class TemplateId(id: Int)

object TemplateId {

  trait Generator {
    def apply(): TemplateId
  }

  object Generator {

    def sequential: Generator = {
      val iter = Iterator.iterate(1)(_ + 1).map(TemplateId(_))
      () => iter.next()
    }
  }
}

case class NodeTemplate(
  id: TemplateId,
  name: Option[String],
  labels: Seq[String]
)

case class RelationshipTemplate(
  id: TemplateId,
  name: Option[String],
  relType: Option[String],
  from: TemplateId,
  to: TemplateId,
  directedness: Directedness
)

sealed trait Directedness

object Directedness {
  case object Directed extends Directedness
  case object Undirected extends Directedness
}

sealed trait RelDirection

object RelDirection {
  case object Incoming extends RelDirection
  case object Outgoing extends RelDirection
  case object Both extends RelDirection
}

class GraphTemplate {
  private val idGen = TemplateId.Generator.sequential
  private val nodes = ArrayBuffer.empty[NodeTemplate]
  private val rels = ArrayBuffer.empty[RelationshipTemplate]

  private var lastId: TemplateId = _

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: GraphTemplate => this.nodes.equals(other.nodes) && this.rels.equals(other.rels)
      case _                    => false
    }
  }

  def getId = lastId

  def addNode(name: Option[String], labels: Seq[String]): GraphTemplate = {
    name.foreach { name =>
      if (nodes.exists(_.name.contains(name))) {
        throw new IllegalArgumentException(s"Node with name $name already exists in template")
      }
    }
    lastId = idGen()
    nodes += NodeTemplate(lastId, name, labels)
    this
  }

  def addNode(name: String, labels: String*): GraphTemplate = addNode(Some(name), labels)

  def addNode(): GraphTemplate = addNode(None, Seq.empty)

  def addNode(name: String): GraphTemplate = addNode(Some(name), Seq.empty)

  def addNode(labels: Seq[String]): GraphTemplate = addNode(None, labels)

  def addRel(name: String, pair: (String, String)): GraphTemplate = {
    val (from, to) = pair
    addRel(
      nodes.find(_.name.contains(from)).get.id,
      nodes.find(_.name.contains(to)).get.id,
      Some(name),
      None,
      Directedness.Directed
    )
  }

  def addRel(name: String, pair: (String, String), relType: String): GraphTemplate = {
    val (from, to) = pair
    addRel(
      nodes.find(_.name.contains(from)).get.id,
      nodes.find(_.name.contains(to)).get.id,
      Some(name),
      Some(relType),
      Directedness.Directed
    )
  }

  def addRel(pair: (String, String)): GraphTemplate = {
    val (from, to) = pair
    addRel(
      nodes.find(_.name.contains(from)).get.id,
      nodes.find(_.name.contains(to)).get.id,
      None,
      None,
      Directedness.Directed
    )
  }

  def addRel(from: TemplateId, to: TemplateId): GraphTemplate =
    addRel(from, to, Directedness.Directed)

  def addRel(from: TemplateId, to: TemplateId, dir: Directedness): GraphTemplate =
    addRel(from, to, None, None, dir)

  def addRel(from: TemplateId, to: TemplateId, name: String, relType: String): GraphTemplate =
    addRel(from, to, name, relType, Directedness.Directed)

  def addRel(from: TemplateId, to: TemplateId, name: String, relType: String, dir: Directedness): GraphTemplate =
    addRel(from, to, Some(name), Some(relType), dir)

  def addRel(
    from: TemplateId,
    to: TemplateId,
    name: Option[String],
    relType: Option[String],
    dir: Directedness
  ): GraphTemplate = {
    name.foreach { name =>
      if (rels.exists(_.name.contains(name))) {
        throw new IllegalArgumentException(s"Relationship with name $name already exists in template")
      }
    }
    lastId = idGen()
    rels += RelationshipTemplate(lastId, name, relType, from, to, dir)
    this
  }

  def instantiate[Node, Rel](instantiator: TemplateInstantiator[Node, Rel]): NamedEntites[Node, Rel] = {
    val nodesByNameBuilder = Map.newBuilder[String, Node]
    val nodesById = {
      val builder = Map.newBuilder[TemplateId, Node]
      for (node <- nodes) {
        val n = instantiator.createNode(node.labels)
        builder += (node.id -> n)
        node.name.foreach(name => nodesByNameBuilder += (name -> n))
      }
      builder.result()
    }

    val relsByNameBuilder = Map.newBuilder[String, Rel]
    for (rel <- rels) {
      val r = instantiator.createRel(nodesById(rel.from), nodesById(rel.to), rel.relType)
      rel.name.foreach(name => relsByNameBuilder += (name -> r))
    }

    NamedEntites(
      nodesByNameBuilder.result(),
      relsByNameBuilder.result()
    )
  }
}

trait TemplateInstantiator[NODE, REL] {
  def createNode(labels: Seq[String]): NODE
  def createRel(from: NODE, to: NODE, relType: Option[String]): REL
}

case class NamedEntites[NODE, REL](
  namedNodes: Map[String, NODE],
  namedRels: Map[String, REL]
) {
  infix def node(name: String): NODE = namedNodes(name)
  infix def rel(name: String): REL = namedRels(name)
}

class GraphTemplateTest extends CypherFunSuite {

  test("node label propagated") {
    val template = new GraphTemplate()
      .addNode("a", "L1", "L2")

    val labels = template.instantiate(new Graph())
      .node("a")
      .labels

    labels.should(contain).only("L1", "L2")
  }

  test("rel type propagated") {
    val template = new GraphTemplate()
      .addNode("a")
      .addNode("b")
      .addRel("r", "a" -> "b", "R1")

    val relType = template.instantiate(new Graph())
      .rel("r")
      .relType

    relType shouldBe Some("R1")
  }

  test("rel from propagated") {
    val template = new GraphTemplate()
      .addNode("a")
      .addNode("b")
      .addRel("r", "a" -> "b")

    val res = template.instantiate(new Graph())

    res.rel("r").from shouldBe res.node("a")
  }

  test("rel to propagated") {
    val template = new GraphTemplate()
      .addNode("a")
      .addNode("b")
      .addRel("r", "a" -> "b")

    val res = template.instantiate(new Graph())

    res.rel("r").to shouldBe res.node("b")
  }

  private class Node(val labels: Seq[String])

  private class Rel(val from: Node, val to: Node, val relType: Option[String])

  private class Graph extends TemplateInstantiator[Node, Rel] {
    val nodes = ArrayBuffer.empty[Node]
    val rels = ArrayBuffer.empty[Rel]

    def createNode(labels: Seq[String]): Node = {
      val n = new Node(labels)
      nodes += n
      n
    }

    def createRel(from: Node, to: Node, relType: Option[String]): Rel = {
      val r = new Rel(from, to, relType)
      rels += r
      r
    }
  }
}
