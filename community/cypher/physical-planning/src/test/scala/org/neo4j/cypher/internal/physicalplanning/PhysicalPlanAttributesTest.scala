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
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.planner.spi.LeafStability
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.EffectiveCardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.LeveragedOrders
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.StableLeafPlans
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class PhysicalPlanAttributesTest extends CypherFunSuite {

  private def attributes(): PhysicalPlanAttributes = PhysicalPlanAttributes(
    new EffectiveCardinalities,
    new ProvidedOrders,
    new LeveragedOrders,
    new StableLeafPlans
  )

  test("copyAll carries the stable-leaf marking to the new id") {
    val attrs = attributes()
    attrs.stableLeafPlans.set(Id(0), LeafStability.MvccEmptyTx)

    attrs.copyAll(Id(0), Id(1))

    attrs.stableLeafPlans.get(Id(1)) shouldBe LeafStability.MvccEmptyTx
  }

  test("copyTo carries the stable-leaf marking from the selected source id") {
    val attrs = attributes()
    attrs.stableLeafPlans.set(Id(7), LeafStability.MvccEmptyTx)

    attrs.copyTo(Id(9))(_ => Id(7))

    attrs.stableLeafPlans.get(Id(9)) shouldBe LeafStability.MvccEmptyTx
  }

  test("copying an unmarked id leaves the target unmarked") {
    val attrs = attributes()

    attrs.copyAll(Id(0), Id(1))

    attrs.stableLeafPlans.get(Id(1)) shouldBe LeafStability.NonMvcc
  }
}
