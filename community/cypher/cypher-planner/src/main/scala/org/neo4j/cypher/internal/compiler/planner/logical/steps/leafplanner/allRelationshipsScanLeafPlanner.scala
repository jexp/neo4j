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

import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.leafplanner.RelationshipLeafPlanner.planHiddenSelectionAndRelationshipLeafPlan
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

case object allRelationshipsScanLeafPlanner extends LeafPlanner {

  override def apply(
    queryGraph: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext
  ): Set[LogicalPlan] = {
    def shouldIgnore(pattern: PatternRelationship): Boolean =
      queryGraph.argumentIds.contains(pattern.variable)

    // We should not plan a relationship scan if there are dynamic relationship type scans that cover the same relationships
    lazy val dynamicTypeScans = DynamicRelationshipTypeLookupLeafPlanner.collectDynamicRelationshipTypeLookupDetails(
      context,
      queryGraph,
      interestingOrderConfig
    )

    queryGraph.patternRelationships.flatMap {

      case relationship @ PatternRelationship(rel, (_, _), _, types, SimplePatternLength)
        if !shouldIgnore(relationship) &&
          types.isEmpty &&
          dynamicTypeScans.isEmpty =>
        Some(planHiddenSelectionAndRelationshipLeafPlan(
          queryGraph.argumentIds,
          relationship,
          context,
          context.staticComponents.logicalPlanProducer.planAllRelationshipsScan(
            rel,
            _,
            _,
            _,
            queryGraph.argumentIds,
            context
          )
        ))

      case _ => None
    }
  }
}
