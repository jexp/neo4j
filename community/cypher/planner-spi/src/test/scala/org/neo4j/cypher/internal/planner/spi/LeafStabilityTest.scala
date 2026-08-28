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
package org.neo4j.cypher.internal.planner.spi

import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class LeafStabilityTest extends CypherFunSuite {

  test("only MvccEmptyTx excludes changes from this transaction") {
    LeafStability.MvccEmptyTx.includeChangesFromThisTransaction shouldBe false
    LeafStability.MvccNonEmptyTx.includeChangesFromThisTransaction shouldBe true
    LeafStability.NonMvcc.includeChangesFromThisTransaction shouldBe true
  }

  test("every case is covered, so a new case forces a decision here") {
    LeafStability.values.length shouldBe 3
  }

  test("an unmarked plan id includes changes from this transaction") {
    new PlanningAttributes.StableLeafPlans().includeChangesFromThisTransaction(Id(0)) shouldBe true
  }

  test("a plan id marked MvccEmptyTx excludes changes from this transaction") {
    val stableLeafPlans = new PlanningAttributes.StableLeafPlans()
    stableLeafPlans.set(Id(0), LeafStability.MvccEmptyTx)

    stableLeafPlans.includeChangesFromThisTransaction(Id(0)) shouldBe false
  }
}
