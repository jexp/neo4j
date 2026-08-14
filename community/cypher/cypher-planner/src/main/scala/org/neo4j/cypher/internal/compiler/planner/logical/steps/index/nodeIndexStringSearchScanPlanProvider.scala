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
package org.neo4j.cypher.internal.compiler.planner.logical.steps.index

import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.steps.leafplanner.index.NodeIndexLeafPlanner.NodeIndexMatch
import org.neo4j.cypher.internal.expressions.Contains
import org.neo4j.cypher.internal.expressions.EndsWith
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

object nodeIndexStringSearchScanPlanProvider extends NodeIndexPlanProvider {

  override def createPlans(
    indexMatches: Set[NodeIndexMatch],
    queryGraph: QueryGraph,
    context: LogicalPlanningContext
  ): Set[LogicalPlan] = for {
    indexMatch <- indexMatches
    if indexMatch.indexDescriptor.properties.size == 1
    plan <- doCreatePlans(indexMatch, queryGraph, context)
  } yield plan

  private def doCreatePlans(
    indexMatch: NodeIndexMatch,
    queryGraph: QueryGraph,
    context: LogicalPlanningContext
  ): Set[LogicalPlan] = {
    def indexableExprWithSearchMode(indexPredicate: IndexCompatiblePredicate): Option[(Expression, StringSearchMode)] =
      indexPredicate.predicate match {
        case Contains(_, valueExpr) if valueExpr.dependencies.subsetOf(queryGraph.argumentIds) =>
          Some((valueExpr, ContainsSearchMode))
        case EndsWith(_, valueExpr) if valueExpr.dependencies.subsetOf(queryGraph.argumentIds) =>
          Some((valueExpr, EndsWithSearchMode))
        case _ => None
      }

    indexMatch.propertyPredicates.flatMap { indexPredicate =>
      indexableExprWithSearchMode(indexPredicate).map {
        case (valueExpr: Expression, stringSearchMode: StringSearchMode) =>
          val singlePredicateSet =
            indexMatch.predicateSet(Seq(indexPredicate), exactPredicatesCanGetValue = false, context, queryGraph)

          val hint = singlePredicateSet
            .fulfilledHints(queryGraph.hints, indexMatch.indexDescriptor.indexType, planIsScan = true)
            .headOption

          context.staticComponents.logicalPlanProducer.planNodeIndexStringSearchScan(
            variable = indexMatch.variable,
            label = indexMatch.labelToken,
            properties = singlePredicateSet.indexedProperties(context),
            stringSearchMode = stringSearchMode,
            solvedPredicates = singlePredicateSet.allSolvedPredicates,
            solvedHint = hint,
            valueExpr = valueExpr,
            argumentIds = queryGraph.argumentIds,
            providedOrder = indexMatch.providedOrder,
            indexOrder = indexMatch.indexOrder,
            context = context,
            indexType = indexMatch.indexDescriptor.indexType
          )
      }
    }.toSet
  }
}
