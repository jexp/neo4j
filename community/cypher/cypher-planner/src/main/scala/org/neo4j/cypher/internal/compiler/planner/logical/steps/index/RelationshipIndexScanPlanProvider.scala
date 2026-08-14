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
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexScanPlanProvider.Solution
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexScanPlanProvider.mergeSolutions
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexScanPlanProvider.predicatesForIndexScan
import org.neo4j.cypher.internal.compiler.planner.logical.steps.leafplanner.RelationshipLeafPlanner.planHiddenSelectionAndRelationshipLeafPlan
import org.neo4j.cypher.internal.compiler.planner.logical.steps.leafplanner.index.RelationshipIndexLeafPlanner.RelationshipIndexMatch
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.RelationshipTypeToken
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.internal.kernel.api.PropertyIndexQuery.allEntries

object RelationshipIndexScanPlanProvider extends RelationshipIndexPlanProvider {

  /**
   * Container for all values that define a RelationshipIndexScan plan
   */
  case class RelationshipIndexScanParameters(
    variable: LogicalVariable,
    token: RelationshipTypeToken,
    patternRelationship: PatternRelationship,
    properties: Seq[IndexedProperty],
    argumentIds: Set[LogicalVariable],
    indexOrder: IndexOrder,
    supportPartitionedScan: Boolean
  )

  override def createPlans(
    indexMatches: Set[RelationshipIndexMatch],
    queryGraph: QueryGraph,
    context: LogicalPlanningContext
  ): Set[LogicalPlan] = {

    val solutions = for {
      indexMatch <- indexMatches
    } yield createSolution(indexMatch, queryGraph, context)

    def provideRelationshipLeafPlan(solution: Solution[RelationshipIndexScanParameters])(
      patternForLeafPlan: PatternRelationship,
      originalPattern: PatternRelationship,
      hiddenSelections: Seq[Expression]
    ): LogicalPlan =
      context.staticComponents.logicalPlanProducer.planRelationshipIndexScan(
        variable = solution.indexScanParameters.variable,
        relationshipType = solution.indexScanParameters.token,
        patternForLeafPlan = patternForLeafPlan,
        originalPattern = originalPattern,
        properties = solution.indexScanParameters.properties,
        solvedPredicates = solution.solvedPredicates,
        solvedHint = solution.solvedHint,
        hiddenSelections = hiddenSelections,
        argumentIds = queryGraph.argumentIds,
        providedOrder = solution.providedOrder,
        indexOrder = solution.indexScanParameters.indexOrder,
        context = context,
        indexType = solution.indexType,
        supportPartitionedScan = solution.indexScanParameters.supportPartitionedScan
      )

    mergeSolutions(solutions) map { solution =>
      planHiddenSelectionAndRelationshipLeafPlan(
        queryGraph.argumentIds,
        solution.indexScanParameters.patternRelationship,
        context,
        provideRelationshipLeafPlan(solution) _
      )
    }
  }

  private def createSolution(
    indexMatch: RelationshipIndexMatch,
    queryGraph: QueryGraph,
    context: LogicalPlanningContext
  ): Solution[RelationshipIndexScanParameters] = {
    val predicateSet =
      indexMatch.predicateSet(
        predicatesForIndexScan(indexMatch.indexDescriptor.indexType, indexMatch.propertyPredicates),
        exactPredicatesCanGetValue = false,
        context,
        queryGraph
      )

    val hint = predicateSet
      .fulfilledHints(queryGraph.hints, indexMatch.indexDescriptor.indexType, planIsScan = true)
      .headOption

    Solution(
      RelationshipIndexScanParameters(
        variable = indexMatch.variable,
        token = indexMatch.relationshipTypeToken,
        patternRelationship = indexMatch.patternRelationship,
        properties = predicateSet.indexedProperties(context),
        argumentIds = queryGraph.argumentIds,
        indexOrder = indexMatch.indexOrder,
        supportPartitionedScan =
          indexMatch.indexDescriptor.maybeKernelIndexCapability.exists(_.supportPartitionedScan(allEntries()))
      ),
      solvedPredicates = predicateSet.allSolvedPredicates,
      solvedHint = hint,
      providedOrder = indexMatch.providedOrder,
      indexType = indexMatch.indexDescriptor.indexType
    )
  }
}
