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
package org.neo4j.cypher.internal.compiler.planner.logical.cardinality.assumeIndependence

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.planner.Optimisation
import org.neo4j.cypher.internal.compiler.planner.Optimisation.MergeLabelInfo
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.LabelInfo
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir.NodeConnection
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QuantifiedPathPattern
import org.neo4j.cypher.internal.ir.SelectivePathPattern
import org.neo4j.cypher.internal.ir.helpers.CachedFunction
import org.neo4j.cypher.internal.options.CypherInferSchemaPartsOption
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.IterableHelper.RichIterableOnce
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.RelTypeId

import scala.annotation.tailrec

trait LabelInferenceStrategy {

  def inferLabels(
    semanticTable: SemanticTable,
    labelInfo: LabelInfo,
    nodeConnections: Seq[NodeConnection]
  ): (LabelInfo, SemanticTable)

  def inferLabels(
    context: LogicalPlanningContext,
    labelInfo: LabelInfo,
    nodeConnections: Seq[NodeConnection]
  ): (LabelInfo, LogicalPlanningContext)

  def inferLabels(
    context: QueryGraphCardinalityContext,
    labelInfo: LabelInfo,
    nodeConnections: Seq[NodeConnection]
  ): (LabelInfo, QueryGraphCardinalityContext)
}

object LabelInferenceStrategy {

  def fromConfig(
    planContext: PlanContext,
    labelInferenceOption: CypherInferSchemaPartsOption,
    plannerVersionOptimisations: Set[Optimisation]
  ): LabelInferenceStrategy = {
    if (
      labelInferenceOption == CypherInferSchemaPartsOption.mostSelectiveLabel ||
      plannerVersionOptimisations.contains(MergeLabelInfo)
    )
      new InferOnlyIfNoOtherLabel(planContext)
    else
      NoInference
  }

  // Don't infer labels for relationships with many types, as it might get too expensive.
  val REL_TYPE_LIMIT = 8

  case object NoInference extends LabelInferenceStrategy {

    override def inferLabels(
      semanticTable: SemanticTable,
      labelInfo: LabelInfo,
      nodeConnections: Seq[NodeConnection]
    ): (LabelInfo, SemanticTable) = {
      (labelInfo, semanticTable)
    }

    override def inferLabels(
      context: LogicalPlanningContext,
      labelInfo: LabelInfo,
      nodeConnections: Seq[NodeConnection]
    ): (LabelInfo, LogicalPlanningContext) = {
      (labelInfo, context)
    }

    override def inferLabels(
      context: QueryGraphCardinalityContext,
      labelInfo: LabelInfo,
      nodeConnections: Seq[NodeConnection]
    ): (LabelInfo, QueryGraphCardinalityContext) = (labelInfo, context)
  }

  final class InferOnlyIfNoOtherLabel(planContext: PlanContext) extends LabelInferenceStrategy {

    private val cachedLabelName: Int => String = CachedFunction(planContext.getLabelName)

    private val cachedInferRelationshipLabels: RelTypeId => Option[RelationshipLabels] =
      CachedFunction { relationshipTypeId =>
        val mostCommonLabels = planContext.statistics.mostCommonLabelGivenRelationshipType(relationshipTypeId.id)
        if (mostCommonLabels.nonEmpty) {
          // first retrieve the cardinality of the relationship type with any node labels, e.g.: |()-[:R]->()|
          val baselineCardinality =
            planContext.statistics.patternStepCardinality(None, Some(relationshipTypeId), None).amount
          val labelsOnStartNode = Set.newBuilder[LabelId]
          val labelsOnEndNode = Set.newBuilder[LabelId]
          // then, for each potentially inferable node label
          mostCommonLabels.foreach { unwrappedLabelId =>
            val labelId = LabelId(unwrappedLabelId)
            // retrieve the cardinality of the same relationship with the label on the start node, e.g.: |(:A)-[:R]->()|
            val withStartNodeCardinality =
              planContext.statistics.patternStepCardinality(Some(labelId), Some(relationshipTypeId), None).amount
            // if it is equal to the baseline cardinality, in our example: if |(:A)-[:R]->()| = |()-[:R]->()|
            if (withStartNodeCardinality == baselineCardinality) {
              // then all relationships with type `relationshipTypeId` will have label `labelId` on their start node:
              // ()-[:R]->() implies (:A)-[:R]->()
              labelsOnStartNode.addOne(labelId)
            }
            // same logic for the end node, note that the same label may be inferred on both the start and the end node
            val withEndNodeCardinality =
              planContext.statistics.patternStepCardinality(None, Some(relationshipTypeId), Some(labelId)).amount
            if (withEndNodeCardinality == baselineCardinality) {
              labelsOnEndNode.addOne(labelId)
            }
          }
          RelationshipLabels.nonEmpty(onStartNode = labelsOnStartNode.result(), onEndNode = labelsOnEndNode.result())
        } else {
          None
        }
      }

    override def inferLabels(
      semanticTable: SemanticTable,
      labelInfo: LabelInfo,
      nodeConnections: Seq[NodeConnection]
    ): (LabelInfo, SemanticTable) =
      nodeConnections
        .view
        .flatMap(inferLabelsForNodeConnection(semanticTable, labelInfo, _))
        .reduceLeftOption(_ union _)
        .map(resolveInferredLabels(semanticTable, labelInfo, _))
        .getOrElse((labelInfo, semanticTable))

    override def inferLabels(
      context: QueryGraphCardinalityContext,
      labelInfo: LabelInfo,
      nodeConnections: Seq[NodeConnection]
    ): (LabelInfo, QueryGraphCardinalityContext) = {
      val (updatedLabelInfo, updatedSemanticTable) = inferLabels(context.semanticTable, labelInfo, nodeConnections)
      (updatedLabelInfo, context.copy(semanticTable = updatedSemanticTable))
    }

    override def inferLabels(
      context: LogicalPlanningContext,
      labelInfo: LabelInfo,
      nodeConnections: Seq[NodeConnection]
    ): (LabelInfo, LogicalPlanningContext) = {
      val (updatedLabelInfo, updatedSemanticTable) = inferLabels(context.semanticTable, labelInfo, nodeConnections)
      (updatedLabelInfo, context.withUpdatedSemanticTable(updatedSemanticTable))
    }

    private def inferLabelsForNodeConnection(
      semanticTable: SemanticTable,
      labelInfo: LabelInfo,
      nodeConnection: NodeConnection
    ): Option[LabelsPerNode] =
      nodeConnection match {
        case relationship: PatternRelationship =>
          lazy val noLabelsOnLeft = labelInfo.get(relationship.left).forall(_.isEmpty)
          lazy val noLabelsOnRight = labelInfo.get(relationship.right).forall(_.isEmpty)
          if (
            relationship.types.nonEmpty &&
            relationship.types.size <= REL_TYPE_LIMIT &&
            (noLabelsOnLeft || noLabelsOnRight)
          )
            inferLabelsForRelationship(semanticTable, relationship)
          else
            None
        case _: QuantifiedPathPattern => None
        case _: SelectivePathPattern  => None
      }

    private def inferLabelsForRelationship(
      semanticTable: SemanticTable,
      relationship: PatternRelationship
    ): Option[LabelsPerNode] =
      relationship.types
        .traverse(semanticTable.id)
        .flatMap(inferRelationshipLabelsIntersection)
        .map(attachLabelsToNodes(relationship.left, relationship.right, relationship.dir, _))

    private def inferRelationshipLabelsIntersection(relationshipTypes: Seq[RelTypeId]): Option[RelationshipLabels] = {
      @tailrec
      def recursively(
        remainingRelationshipTypes: Seq[RelTypeId],
        intersection: RelationshipLabels
      ): Option[RelationshipLabels] = {
        if (remainingRelationshipTypes.isEmpty) {
          // if there aren't any other types to process, return the intersection
          Some(intersection)
        } else {
          val head = remainingRelationshipTypes.head
          val tail = remainingRelationshipTypes.tail
          cachedInferRelationshipLabels(head).flatMap(intersection.intersect) match {
            case Some(newIntersection) =>
              // if the intersection between the labels inferred so far and the ones inferred on the first remaining relationship is defined, keep recursing
              recursively(tail, newIntersection)
            case None =>
              None // otherwise stop here, ε ∩ _ = ε
          }
        }
      }

      if (relationshipTypes.isEmpty) {
        None // no types, no labels, simple
      } else {
        val head = relationshipTypes.head
        val tail = relationshipTypes.tail
        cachedInferRelationshipLabels(head) match {
          case Some(inferredLabelsOnHead) =>
            // if labels can be inferred on the first relationship, calculate the intersection with the other types
            recursively(tail, inferredLabelsOnHead)
          case None =>
            None // otherwise stop here, ε ∩ _ = ε
        }
      }
    }

    private def attachLabelsToNodes(
      leftNode: LogicalVariable,
      rightNode: LogicalVariable,
      direction: SemanticDirection,
      relationshipLabels: RelationshipLabels
    ): LabelsPerNode =
      direction match {
        case SemanticDirection.OUTGOING =>
          LabelsPerNode.union(
            leftNode -> relationshipLabels.onStartNode,
            rightNode -> relationshipLabels.onEndNode
          )
        case SemanticDirection.INCOMING =>
          LabelsPerNode.union(
            leftNode -> relationshipLabels.onEndNode,
            rightNode -> relationshipLabels.onStartNode
          )
        case SemanticDirection.BOTH =>
          val intersection = relationshipLabels.onStartNode.intersect(relationshipLabels.onEndNode)
          LabelsPerNode.union(
            leftNode -> intersection,
            rightNode -> intersection
          )
      }

    private def resolveInferredLabels(
      semanticTable: SemanticTable,
      labelInfo: LabelInfo,
      inferredLabels: LabelsPerNode
    ): (LabelInfo, SemanticTable) = {
      val inferredLabelInfoAndResolvedLabelNames = for {
        (node, labelIds) <- inferredLabels.values.view
        if labelInfo.get(node).forall(_.isEmpty)
        inferredLabelId <- labelIds.minByOption { labelId =>
          planContext.statistics.nodesWithLabelCardinality(Some(labelId))
        }
        labelName = cachedLabelName(inferredLabelId)
        labelInfoEntry = (node, Set(LabelName(labelName)(InputPosition.NONE)))
        resolvedLabelName = (labelName, inferredLabelId)
      } yield (labelInfoEntry, resolvedLabelName)

      val (inferredLabelInfo, resolvedLabelNames) = inferredLabelInfoAndResolvedLabelNames.unzip

      val newLabelInfo = labelInfo ++ inferredLabelInfo

      val newlyResolvedLabelNames = resolvedLabelNames.filterNot { case (labelName, _) =>
        semanticTable.resolvedLabelNames.contains(labelName)
      }

      // calculating the hashcode of a semantic table is expensive, we want to create a new instance only if necessary
      val newSemanticTable =
        if (newlyResolvedLabelNames.nonEmpty)
          semanticTable.addResolvedLabelNames(newlyResolvedLabelNames)
        else
          semanticTable

      (newLabelInfo, newSemanticTable)
    }
  }

  final private case class LabelsPerNode(values: Map[LogicalVariable, Set[LabelId]]) extends AnyVal {

    infix def union(other: LabelsPerNode): LabelsPerNode =
      LabelsPerNode.buildUnion(values.view ++ other.values)
  }

  private object LabelsPerNode {

    def union(elems: (LogicalVariable, Set[LabelId])*): LabelsPerNode =
      buildUnion(elems.view.filter(_._2.nonEmpty))

    private def buildUnion(elems: Iterable[(LogicalVariable, Set[LabelId])]): LabelsPerNode =
      LabelsPerNode(elems.groupMapReduce(_._1)(_._2)(_ union _))
  }

  final private case class RelationshipLabels(onStartNode: Set[LabelId], onEndNode: Set[LabelId]) {

    def intersect(other: RelationshipLabels): Option[RelationshipLabels] =
      RelationshipLabels.nonEmpty(
        onStartNode = onStartNode.intersect(other.onStartNode),
        onEndNode = onEndNode.intersect(other.onEndNode)
      )
  }

  private object RelationshipLabels {

    def nonEmpty(onStartNode: Set[LabelId], onEndNode: Set[LabelId]): Option[RelationshipLabels] =
      Option.when(onStartNode.nonEmpty || onEndNode.nonEmpty)(RelationshipLabels(onStartNode, onEndNode))
  }
}
