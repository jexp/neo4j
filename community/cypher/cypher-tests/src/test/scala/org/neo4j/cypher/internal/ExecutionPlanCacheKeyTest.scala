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
package org.neo4j.cypher.internal

import org.neo4j.cypher.CommunityCypherTestSuite
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.cache.CypherQueryCaches.ExecutionPlanCacheKey
import org.neo4j.cypher.internal.compiler.phases.CachablePlanningAttributes
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.planner.spi.ImmutablePlanningAttributes
import org.neo4j.cypher.internal.planner.spi.LeafStability
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.StableLeafPlans
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.attribution.SameId

/**
 * The stable-iterator classification of a leaf depends on the transaction that planned the query, not on the query
 * text, so an execution plan compiled for one classification must never be served to a transaction that was
 * classified differently.
 */
class ExecutionPlanCacheKeyTest extends CommunityCypherTestSuite with AstConstructionTestSupport {

  private val leafId = Id(0)
  private val logicalPlan = AllNodesScan(varFor("n"), Set.empty)(SameId(leafId))

  test("execution plan cache key distinguishes stable-leaf markings") {
    cacheKeyWith(marked(LeafStability.MvccEmptyTx)) should not equal
      cacheKeyWith(marked(LeafStability.MvccNonEmptyTx))
  }

  test("execution plan cache key distinguishes a marked leaf from an unmarked one") {
    cacheKeyWith(marked(LeafStability.MvccEmptyTx)) should not equal cacheKeyWith(new StableLeafPlans)
  }

  test("execution plan cache keys are equal when the stable-leaf markings agree") {
    cacheKeyWith(marked(LeafStability.MvccEmptyTx)) shouldEqual cacheKeyWith(marked(LeafStability.MvccEmptyTx))
  }

  private def marked(stability: LeafStability): StableLeafPlans = {
    val stableLeafPlans = new StableLeafPlans
    stableLeafPlans.set(leafId, stability)
    stableLeafPlans
  }

  private def cacheKeyWith(stableLeafPlans: StableLeafPlans): ExecutionPlanCacheKey =
    ExecutionPlanCacheKey(
      runtimeKey = "",
      logicalPlan,
      CachablePlanningAttributes(
        ImmutablePlanningAttributes.EffectiveCardinalities(new PlanningAttributes.EffectiveCardinalities),
        ImmutablePlanningAttributes.ProvidedOrders(new PlanningAttributes.ProvidedOrders),
        ImmutablePlanningAttributes.LeveragedOrders(new PlanningAttributes.LeveragedOrders),
        ImmutablePlanningAttributes.StableLeafPlans(stableLeafPlans),
        new PlanningAttributes.LabelAndRelTypeInfos,
        new PlanningAttributes.CachedPropertiesPerPlan,
        readOnly = false
      ).cacheKey,
      CypherVersion.Cypher25
    )
}
