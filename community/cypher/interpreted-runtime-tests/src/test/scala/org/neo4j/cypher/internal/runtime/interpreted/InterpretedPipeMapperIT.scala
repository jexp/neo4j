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
package org.neo4j.cypher.internal.runtime.interpreted

import org.mockito.Mockito.atLeastOnce
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.PropertyKeyToken
import org.neo4j.cypher.internal.expressions.RELATIONSHIP_TYPE
import org.neo4j.cypher.internal.expressions.RelationshipTypeToken
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.DirectedAllRelationshipsScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.DirectedUnionRelationshipTypesScan
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandInto
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.ManySeekableArgs
import org.neo4j.cypher.internal.logical.plans.NodeByIdSeek
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.logical.plans.NodeIndexScan
import org.neo4j.cypher.internal.logical.plans.OptionalExpand
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.UnionNodeByLabelsScan
import org.neo4j.cypher.internal.planner.spi.LeafStability
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.StableLeafPlans
import org.neo4j.cypher.internal.planner.spi.ReadTokenContext
import org.neo4j.cypher.internal.runtime.CypherRuntimeConfiguration
import org.neo4j.cypher.internal.runtime.ParameterMapping
import org.neo4j.cypher.internal.runtime.QueryIndexRegistrator
import org.neo4j.cypher.internal.runtime.SelectivityTrackerRegistrator
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.CommunityExpressionConverter
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.KeyToken.Resolved
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.TokenType
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AllNodesScanPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ArgumentPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.CartesianProductPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.DirectedAllRelationshipsScanPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.DirectedRelationshipByIdSeekPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.DirectedRelationshipIndexScanPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.DirectedRelationshipTypeScanPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.DirectedUnionRelationshipTypesScanPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.DistinctPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ExpandAllPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ExpandIntoPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyLabel
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ManySeekArgs
import org.neo4j.cypher.internal.runtime.interpreted.pipes.NodeByIdSeekPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.NodeByLabelScanPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.NodeHashJoinPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.NodeIndexScanPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.OptionalExpandIntoPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeTreeBuilder
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ProjectionPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RelationshipTypes
import org.neo4j.cypher.internal.runtime.interpreted.pipes.SingleSeekArg
import org.neo4j.cypher.internal.runtime.interpreted.pipes.UndirectedRelationshipByIdSeekPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.UnionNodeByLabelsScanPipe
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.attribution.SequentialIdGen
import org.neo4j.graphdb.schema.IndexType
import org.neo4j.values.storable.Values.intValue

class InterpretedPipeMapperIT extends InterpretedRuntimeTestSuite with AstConstructionTestSupport {
  implicit private val idGen: SequentialIdGen = new SequentialIdGen()

  private val planContext: PlanContext = mock[PlanContext]

  private val semanticTable = new SemanticTable(resolvedRelTypeNames =
    Map("existing1" -> RelTypeId(1), "existing2" -> RelTypeId(2), "existing3" -> RelTypeId(3))
  )

  private val converters =
    new ExpressionConverters(
      None,
      CommunityExpressionConverter(
        ReadTokenContext.EMPTY,
        new AnonymousVariableNameGenerator(),
        new SelectivityTrackerRegistrator(),
        CypherRuntimeConfiguration.defaultConfiguration,
        CypherVersion.Legacy.legacyVersion(),
        mock[QueryIndexRegistrator]
      )
    )

  private def pipeMapperWith(stableLeafPlans: StableLeafPlans) =
    InterpretedPipeMapper(
      CypherVersion.Legacy.legacyVersion(),
      readOnly = true,
      converters,
      planContext,
      mock[QueryIndexRegistrator],
      new AnonymousVariableNameGenerator(),
      isCommunity = true,
      ParameterMapping.empty,
      stableLeafPlans
    )(semanticTable)

  private val pipeMapper = pipeMapperWith(new StableLeafPlans)

  private def build(logicalPlan: LogicalPlan): Pipe =
    PipeTreeBuilder(pipeMapper).build(logicalPlan, CancellationChecker.neverCancelled(), isNestedPlan = false)

  private def buildWith(stableLeafPlans: StableLeafPlans)(logicalPlan: LogicalPlan): Pipe =
    PipeTreeBuilder(pipeMapperWith(stableLeafPlans))
      .build(logicalPlan, CancellationChecker.neverCancelled(), isNestedPlan = false)

  private def markedAt(id: Id, stability: LeafStability): StableLeafPlans = {
    val stableLeafPlans = new StableLeafPlans
    stableLeafPlans.set(id, stability)
    stableLeafPlans
  }

  test("projection only query") {
    val logicalPlan = Projection(
      Argument(),
      Map(varFor("42") -> literalInt(42))
    )
    val pipe = build(logicalPlan)

    pipe should equal(ProjectionPipe(ArgumentPipe()(), Map("42" -> commands.expressions.Literal(intValue(42)))))
  }

  test("simple pattern query") {
    val logicalPlan = AllNodesScan(varFor("n"), Set.empty)
    val pipe = build(logicalPlan)

    pipe should equal(AllNodesScanPipe("n", includeChangesFromThisTransaction = true)())
  }

  test("simple label scan query") {
    val logicalPlan = NodeByLabelScan(varFor("n"), labelName("Foo"), Set.empty, IndexOrderAscending)
    val pipe = build(logicalPlan)

    pipe should equal(
      NodeByLabelScanPipe("n", LazyLabel("Foo"), IndexOrderAscending, includeChangesFromThisTransaction = true)()
    )
  }

  test("simple node by id seek query") {
    val astLiteral = listOfInt(42)
    val logicalPlan = NodeByIdSeek(varFor("n"), ManySeekableArgs(astLiteral), Set.empty)
    val pipe = build(logicalPlan)

    pipe should equal(NodeByIdSeekPipe("n", SingleSeekArg(commands.expressions.Literal(intValue(42))))())
  }

  test("simple node by id seek query with multiple values") {
    val astCollection = listOfInt(42, 43, 43)
    val logicalPlan = NodeByIdSeek(varFor("n"), ManySeekableArgs(astCollection), Set.empty)
    val pipe = build(logicalPlan)

    pipe should equal(NodeByIdSeekPipe(
      "n",
      ManySeekArgs(converters.toCommandExpression(logicalPlan.id, astCollection))
    )())
  }

  test("simple relationship by id seek query") {
    val astLiteral = listOfInt(42)
    val fromNode = "from"
    val toNode = "to"
    val logicalPlan = DirectedRelationshipByIdSeek(
      varFor("r"),
      ManySeekableArgs(astLiteral),
      varFor(fromNode),
      varFor(toNode),
      Set.empty
    )
    val pipe = build(logicalPlan)

    pipe should equal(DirectedRelationshipByIdSeekPipe(
      Some("r"),
      SingleSeekArg(commands.expressions.Literal(intValue(42))),
      Some(toNode),
      Some(fromNode)
    )())
  }

  test("simple relationship by id seek query with multiple values") {
    val astCollection = listOfInt(42, 43, 43)

    val fromNode = "from"
    val toNode = "to"
    val logicalPlan = DirectedRelationshipByIdSeek(
      varFor("r"),
      ManySeekableArgs(astCollection),
      varFor(fromNode),
      varFor(toNode),
      Set.empty
    )
    val pipe = build(logicalPlan)

    pipe should equal(DirectedRelationshipByIdSeekPipe(
      Some("r"),
      ManySeekArgs(converters.toCommandExpression(logicalPlan.id, astCollection)),
      Some(toNode),
      Some(fromNode)
    )())
  }

  test("simple undirected relationship by id seek query with multiple values") {
    val astCollection = listOfInt(42, 43, 43)

    val fromNode = "from"
    val toNode = "to"
    val logicalPlan = UndirectedRelationshipByIdSeek(
      varFor("r"),
      ManySeekableArgs(astCollection),
      varFor(fromNode),
      varFor(toNode),
      Set.empty
    )
    val pipe = build(logicalPlan)

    pipe should equal(UndirectedRelationshipByIdSeekPipe(
      Some("r"),
      ManySeekArgs(converters.toCommandExpression(logicalPlan.id, astCollection)),
      Some(toNode),
      Some(fromNode)
    )())
  }

  test("simple cartesian product") {
    val lhs = AllNodesScan(varFor("n"), Set.empty)
    val rhs = AllNodesScan(varFor("m"), Set.empty)
    val logicalPlan = CartesianProduct(lhs, rhs)
    val pipe = build(logicalPlan)

    pipe should equal(CartesianProductPipe(
      AllNodesScanPipe("n", includeChangesFromThisTransaction = true)(),
      AllNodesScanPipe("m", includeChangesFromThisTransaction = true)()
    )())
  }

  test("simple expand") {
    val logicalPlan = Expand(
      AllNodesScan(varFor("a"), Set.empty),
      varFor("a"),
      SemanticDirection.INCOMING,
      Seq(),
      varFor("b"),
      varFor("r1"),
      ExpandAll
    )(idGen)
    val pipe = build(logicalPlan)

    pipe should equal(ExpandAllPipe(
      AllNodesScanPipe("a", includeChangesFromThisTransaction = true)(),
      "a",
      Some("r1"),
      Some("b"),
      SemanticDirection.INCOMING,
      RelationshipTypes.empty
    )())
  }

  test("simple expand into existing variable MATCH a-[r]->a ") {
    val logicalPlan =
      Expand(
        AllNodesScan(varFor("a"), Set.empty),
        varFor("a"),
        SemanticDirection.INCOMING,
        Seq(),
        varFor("a"),
        varFor("r"),
        ExpandInto
      )(idGen)
    val pipe = build(logicalPlan)

    val inner: Pipe =
      ExpandIntoPipe(
        AllNodesScanPipe("a", includeChangesFromThisTransaction = true)(),
        "a",
        Some("r"),
        "a",
        SemanticDirection.INCOMING,
        RelationshipTypes.empty
      )()

    pipe should equal(inner)
  }

  test("optional expand into existing variable MATCH a OPTIONAL MATCH a-[r]->a ") {
    val logicalPlan =
      OptionalExpand(
        AllNodesScan(varFor("a"), Set.empty),
        varFor("a"),
        SemanticDirection.INCOMING,
        Seq(),
        varFor("a"),
        varFor("r"),
        ExpandInto,
        None
      )(idGen)
    val pipe = build(logicalPlan)

    pipe should equal(
      OptionalExpandIntoPipe(
        AllNodesScanPipe("a", includeChangesFromThisTransaction = true)(),
        "a",
        Some("r"),
        "a",
        SemanticDirection.INCOMING,
        RelationshipTypes.empty,
        None
      )()
    )
  }

  test("simple hash join") {
    val logicalPlan =
      NodeHashJoin(
        Set(varFor("b")),
        Expand(
          AllNodesScan(varFor("a"), Set.empty),
          varFor("a"),
          SemanticDirection.INCOMING,
          Seq(),
          varFor("b"),
          varFor("r1"),
          ExpandAll
        ),
        Expand(
          AllNodesScan(varFor("c"), Set.empty),
          varFor("c"),
          SemanticDirection.INCOMING,
          Seq(),
          varFor("b"),
          varFor("r2"),
          ExpandAll
        )
      )
    val pipe = build(logicalPlan)

    pipe should equal(NodeHashJoinPipe(
      Set("b"),
      ExpandAllPipe(
        AllNodesScanPipe("a", includeChangesFromThisTransaction = true)(),
        "a",
        Some("r1"),
        Some("b"),
        SemanticDirection.INCOMING,
        RelationshipTypes.empty
      )(),
      ExpandAllPipe(
        AllNodesScanPipe("c", includeChangesFromThisTransaction = true)(),
        "c",
        Some("r2"),
        Some("b"),
        SemanticDirection.INCOMING,
        RelationshipTypes.empty
      )()
    )())
  }

  test("Aggregation with no aggregating columns => DistinctPipe with resolved expressions") {
    // GIVEN
    val token = 42
    when(planContext.getOptPropertyKeyId("prop")).thenReturn(Some(token))
    val allNodesScan = AllNodesScan(varFor("n"), Set.empty)
    val expressions = Map[LogicalVariable, Expression](varFor("n.prop") -> prop("n", "prop"))
    val aggregation = Aggregation(allNodesScan, expressions, Map.empty)

    // WHEN
    val pipe = build(aggregation)

    // THEN
    verify(planContext, atLeastOnce()).getOptPropertyKeyId("prop")
    pipe should equal(
      DistinctPipe(
        AllNodesScanPipe("n", includeChangesFromThisTransaction = true)(),
        Array(DistinctPipe.GroupingCol(
          "n.prop",
          commands.expressions.Property(
            commands.expressions.Variable("n"),
            Resolved("prop", token, TokenType.PropertyKey)
          )
        ))
      )()
    )
  }

  test("all nodes scan marked MvccEmptyTx excludes transaction state") {
    val logicalPlan = AllNodesScan(varFor("n"), Set.empty)(SameId(Id(0)))

    val pipe = buildWith(markedAt(Id(0), LeafStability.MvccEmptyTx))(logicalPlan)

    pipe should equal(AllNodesScanPipe("n", includeChangesFromThisTransaction = false)())
  }

  test("unmarked all nodes scan includes transaction state") {
    val logicalPlan = AllNodesScan(varFor("n"), Set.empty)(SameId(Id(0)))

    val pipe = build(logicalPlan)

    pipe should equal(AllNodesScanPipe("n", includeChangesFromThisTransaction = true)())
  }

  test("all nodes scan marked MvccNonEmptyTx includes transaction state") {
    val logicalPlan = AllNodesScan(varFor("n"), Set.empty)(SameId(Id(0)))

    val pipe = buildWith(markedAt(Id(0), LeafStability.MvccNonEmptyTx))(logicalPlan)

    pipe should equal(AllNodesScanPipe("n", includeChangesFromThisTransaction = true)())
  }

  test("label scan leaf marked MvccEmptyTx excludes transaction state") {
    val logicalPlan = NodeByLabelScan(varFor("n"), labelName("A"), Set.empty, IndexOrderNone)(SameId(Id(0)))

    val pipe = buildWith(markedAt(Id(0), LeafStability.MvccEmptyTx))(logicalPlan)

    pipe should equal(
      NodeByLabelScanPipe("n", LazyLabel("A"), IndexOrderNone, includeChangesFromThisTransaction = false)()
    )
  }

  test("unmarked label scan leaf includes transaction state") {
    val logicalPlan = NodeByLabelScan(varFor("n"), labelName("A"), Set.empty, IndexOrderNone)(SameId(Id(0)))

    val pipe = build(logicalPlan)

    pipe should equal(
      NodeByLabelScanPipe("n", LazyLabel("A"), IndexOrderNone, includeChangesFromThisTransaction = true)()
    )
  }

  test("union label scan leaf marked MvccEmptyTx excludes transaction state") {
    val logicalPlan =
      UnionNodeByLabelsScan(varFor("n"), Seq(labelName("A"), labelName("B")), Set.empty, IndexOrderNone)(SameId(Id(0)))

    val pipe = buildWith(markedAt(Id(0), LeafStability.MvccEmptyTx))(logicalPlan)

    pipe.asInstanceOf[UnionNodeByLabelsScanPipe].includeChangesFromThisTransaction shouldBe false
  }

  test("unmarked union label scan leaf includes transaction state") {
    val logicalPlan =
      UnionNodeByLabelsScan(varFor("n"), Seq(labelName("A"), labelName("B")), Set.empty, IndexOrderNone)(SameId(Id(0)))

    val pipe = build(logicalPlan)

    pipe.asInstanceOf[UnionNodeByLabelsScanPipe].includeChangesFromThisTransaction shouldBe true
  }

  private def nodeIndexScanAt(id: Id) = NodeIndexScan(
    varFor("n"),
    LabelToken("Awesome", LabelId(0)),
    Seq(IndexedProperty(PropertyKeyToken("prop", PropertyKeyId(0)), DoNotGetValue, NODE_TYPE)),
    Set.empty,
    IndexOrderNone,
    IndexType.RANGE,
    supportPartitionedScan = false
  )(SameId(id))

  test("a node index scan leaf marked MvccEmptyTx excludes transaction state") {
    val pipe = buildWith(markedAt(Id(0), LeafStability.MvccEmptyTx))(nodeIndexScanAt(Id(0)))

    pipe.asInstanceOf[NodeIndexScanPipe].includeChangesFromThisTransaction shouldBe false
  }

  test("an unmarked node index scan leaf includes transaction state") {
    val pipe = build(nodeIndexScanAt(Id(0)))

    pipe.asInstanceOf[NodeIndexScanPipe].includeChangesFromThisTransaction shouldBe true
  }

  private def directedRelationshipTypeScanAt(id: Id) = DirectedRelationshipTypeScan(
    Some(varFor("r")),
    Some(varFor("a")),
    relTypeName("R"),
    Some(varFor("b")),
    Set.empty,
    IndexOrderNone
  )(SameId(id))

  test("relationship type scan leaf marked MvccEmptyTx excludes transaction state") {
    val pipe = buildWith(markedAt(Id(0), LeafStability.MvccEmptyTx))(directedRelationshipTypeScanAt(Id(0)))

    pipe.asInstanceOf[DirectedRelationshipTypeScanPipe].includeChangesFromThisTransaction shouldBe false
  }

  test("unmarked relationship type scan leaf includes transaction state") {
    val pipe = build(directedRelationshipTypeScanAt(Id(0)))

    pipe.asInstanceOf[DirectedRelationshipTypeScanPipe].includeChangesFromThisTransaction shouldBe true
  }

  private def directedUnionRelationshipTypesScanAt(id: Id) = DirectedUnionRelationshipTypesScan(
    varFor("r"),
    varFor("a"),
    Seq(relTypeName("R"), relTypeName("S")),
    varFor("b"),
    Set.empty,
    IndexOrderNone
  )(SameId(id))

  test("union relationship type scan leaf marked MvccEmptyTx excludes transaction state") {
    val pipe = buildWith(markedAt(Id(0), LeafStability.MvccEmptyTx))(directedUnionRelationshipTypesScanAt(Id(0)))

    pipe.asInstanceOf[DirectedUnionRelationshipTypesScanPipe].includeChangesFromThisTransaction shouldBe false
  }

  test("unmarked union relationship type scan leaf includes transaction state") {
    val pipe = build(directedUnionRelationshipTypesScanAt(Id(0)))

    pipe.asInstanceOf[DirectedUnionRelationshipTypesScanPipe].includeChangesFromThisTransaction shouldBe true
  }

  private def directedRelationshipIndexScanAt(id: Id) = DirectedRelationshipIndexScan(
    Some(varFor("r")),
    Some(varFor("a")),
    Some(varFor("b")),
    RelationshipTypeToken("R", RelTypeId(0)),
    Seq(IndexedProperty(PropertyKeyToken("prop", PropertyKeyId(0)), DoNotGetValue, RELATIONSHIP_TYPE)),
    Set.empty,
    IndexOrderNone,
    IndexType.RANGE,
    supportPartitionedScan = false
  )(SameId(id))

  test("relationship index scan leaf marked MvccEmptyTx excludes transaction state") {
    val pipe = buildWith(markedAt(Id(0), LeafStability.MvccEmptyTx))(directedRelationshipIndexScanAt(Id(0)))

    pipe.asInstanceOf[DirectedRelationshipIndexScanPipe].includeChangesFromThisTransaction shouldBe false
  }

  test("unmarked relationship index scan leaf includes transaction state") {
    val pipe = build(directedRelationshipIndexScanAt(Id(0)))

    pipe.asInstanceOf[DirectedRelationshipIndexScanPipe].includeChangesFromThisTransaction shouldBe true
  }

  private def directedAllRelationshipsScanAt(id: Id) = DirectedAllRelationshipsScan(
    Some(varFor("r")),
    Some(varFor("a")),
    Some(varFor("b")),
    Set.empty
  )(SameId(id))

  test("all relationships scan leaf marked MvccEmptyTx excludes transaction state") {
    val pipe = buildWith(markedAt(Id(0), LeafStability.MvccEmptyTx))(directedAllRelationshipsScanAt(Id(0)))

    pipe.asInstanceOf[DirectedAllRelationshipsScanPipe].includeChangesFromThisTransaction shouldBe false
  }

  test("unmarked all relationships scan leaf includes transaction state") {
    val pipe = build(directedAllRelationshipsScanAt(Id(0)))

    pipe.asInstanceOf[DirectedAllRelationshipsScanPipe].includeChangesFromThisTransaction shouldBe true
  }
}
