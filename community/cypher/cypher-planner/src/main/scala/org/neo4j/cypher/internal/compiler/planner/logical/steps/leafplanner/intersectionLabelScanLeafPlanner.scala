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
package org.neo4j.cypher.internal.compiler.planner.logical.steps.leafplanner

import org.neo4j.cypher.internal.compiler.planner.logical.LabelScanLeafPlanner.HintsAndHintedLabels
import org.neo4j.cypher.internal.compiler.planner.logical.LabelScanLeafPlanner.getHintsAndHintedLabels
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.ResultOrdering
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability.BOTH
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.collection.immutable.ListSet
import org.neo4j.cypher.internal.util.collection.immutable.ListSet.IterableOnceToListSet

import scala.collection.mutable

case object intersectionLabelScanLeafPlanner extends LeafPlanner {

  override def apply(
    qg: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext
  ): Set[LogicalPlan] = {
    if (!context.settings.planningIntersectionScansEnabled) {
      Set.empty
    } else {
      context.staticComponents.planContext.nodeTokenIndex match {
        case Some(nodeTokenIndex) if nodeTokenIndex.orderCapability == BOTH =>
          // Combine for example HasLabels(n, Seq(A)), HasLabels(n, Seq(B)) to n -> Set(A, B)
          val combined: Map[Variable, ListSet[LabelName]] = {
            qg.selections.flatPredicatesSet.foldLeft(Map.empty[Variable, ListSet[LabelName]]) {
              case (acc, current) => current match {
                  case HasLabels(variable: Variable, labels)
                    if (qg.patternNodes(variable) && !qg.argumentIds(variable)) =>
                    val newValue = acc.get(variable).map(current => (current ++ labels)).getOrElse(labels.toListSet)
                    acc + (variable -> newValue)
                  case _ => acc
                }
            }
          }
          // We only create one plan with the intersection of all labels, we could change this to generate all combinations, e.g.
          // given labels A, B and C
          // - (A,B,C)
          // - (A,B)
          // - (B,C)
          // - (A, C)
          // and in that way create more flexibility for the planner to plan things like
          //
          //   .nodeHashJoin("x")
          //  .|.intersectionNodeByLabelsScan("n", Seq("B", "C"))
          //  .nodeUniqueIndexSeek("n:A(prop = 42)")
          //
          // Will leave this as a future potential improvement.
          val results = mutable.HashSet.empty[LogicalPlan]
          combined.foreach {
            case (variable, labels) if labels.size > 1 =>
              val providedOrder = ResultOrdering.providedOrderForLabelScan(
                interestingOrderConfig.orderToSolve,
                variable,
                nodeTokenIndex.orderCapability,
                context.providedOrderFactory
              )
              val HintsAndHintedLabels(hints, hintedLabels) =
                getHintsAndHintedLabels(qg.hints, variable, labels)

              val prunedLabels =
                context.staticComponents.graphSchemaOptimizations.pruneImpliedLabels(labels) ++ hintedLabels

              // Only plan an intersection scan if we have more than one label left after pruning.
              // Otherwise, the node label scan provider will take over.
              if (prunedLabels.size > 1) {
                // We have more than one label left after pruning, so we can create an intersection node
                val sortedLabels = prunedLabels.toSeq.sortBy(_.name)
                results += context.staticComponents.logicalPlanProducer.planIntersectNodeByLabelsScan(
                  variable,
                  sortedLabels,
                  Seq(HasLabels(variable, sortedLabels)(InputPosition.NONE)),
                  hints.toSeq,
                  qg.argumentIds,
                  providedOrder,
                  context
                )
              }
            case _ => // do nothing
          }
          results.toSet

        case _ => Set.empty
      }
    }
  }
}
