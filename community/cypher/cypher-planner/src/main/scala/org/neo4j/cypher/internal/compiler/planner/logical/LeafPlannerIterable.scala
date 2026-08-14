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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.leafplanner.LeafPlanner
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

trait LeafPlannerIterable {

  def candidates(
    qg: QueryGraph,
    f: (LogicalPlan, QueryGraph) => LogicalPlan = (plan, _) => plan,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext
  ): Set[LogicalPlan]
}

case class LeafPlannerList(leafPlanners: IndexedSeq[LeafPlanner]) extends LeafPlannerIterable {

  override def candidates(
    qg: QueryGraph,
    f: (LogicalPlan, QueryGraph) => LogicalPlan = (plan, _) => plan,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext
  ): Set[LogicalPlan] = {
    leafPlanners.flatMap(_.apply(qg, interestingOrderConfig, context)).map(f(_, qg)).toSet
  }
}

/**
 * A LeafPlannerIterable that first tries the leaf planners in the priority list. If they produce
 * any candidates, those are returned exclusively — the fallback is not consulted.
 *
 * Any symbols not directly covered by a priority leaf plan are expected to be reachable via
 * expansion along node connections within the same connected component.
 *
 * The fallback is only used when the priority planners produce no candidates at all.
 *
 * @param priority the preferred leaf planners, which will be tried first
 * @param fallback the alternative leaf planners, which will only be tried if the priority leaf planners return no candidates
 */
case class PriorityLeafPlannerList(priority: LeafPlannerIterable, fallback: LeafPlannerIterable)
    extends LeafPlannerIterable {

  override def candidates(
    qg: QueryGraph,
    f: (LogicalPlan, QueryGraph) => LogicalPlan,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext
  ): Set[LogicalPlan] = {
    val priorityPlans = priority.candidates(qg, f, interestingOrderConfig, context)
    if (priorityPlans.nonEmpty) priorityPlans
    else fallback.candidates(qg, f, interestingOrderConfig, context)
  }
}
