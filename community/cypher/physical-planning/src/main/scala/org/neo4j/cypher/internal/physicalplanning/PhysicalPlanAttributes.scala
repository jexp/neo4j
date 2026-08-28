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

import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.EffectiveCardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.LeveragedOrders
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.StableLeafPlans
import org.neo4j.cypher.internal.util.attribution.Id

/**
 * Bundles the mutable plan attributes used by pipelined pre-physical plan rewriters.
 * Provides a unified copyAll(from, to) to ensure all attributes are propagated consistently.
 *
 * Note: PlanningAttributesCacheKey bundles the same 4 attributes in their immutable form
 * for execution plan cache keying — that is a separate concern.
 */
case class PhysicalPlanAttributes(
  effectiveCardinalities: EffectiveCardinalities,
  providedOrders: ProvidedOrders,
  leveragedOrders: LeveragedOrders,
  stableLeafPlans: StableLeafPlans
) {
  import PhysicalPlanAttributes.AttributeType

  def copyAll(from: Id, to: Id): Unit = {
    effectiveCardinalities.copy(from, to)
    providedOrders.copy(from, to)
    leveragedOrders.copy(from, to)
    stableLeafPlans.copy(from, to)
  }

  /**
   * Copies all attributes to the target Id, selecting the source Id for each attribute type
   * via the given selector function. Uses a sealed trait so that Scala's exhaustive pattern
   * matching ensures all attribute types are handled.
   *
   * Example:
   * {{{
   * attributes.copyTo(limit.id) {
   *   case EffectiveCardinalities => lhs.id
   *   case ProvidedOrders | LeveragedOrders => rhs.id
   * }
   * }}}
   */
  def copyTo(target: Id)(fromSelector: AttributeType => Id): Unit = {
    effectiveCardinalities.copy(fromSelector(AttributeType.EffectiveCardinalities), target)
    providedOrders.copy(fromSelector(AttributeType.ProvidedOrders), target)
    leveragedOrders.copy(fromSelector(AttributeType.LeveragedOrders), target)
    stableLeafPlans.copy(fromSelector(AttributeType.StableLeafPlans), target)
  }
}

object PhysicalPlanAttributes {
  sealed trait AttributeType

  object AttributeType {
    case object EffectiveCardinalities extends AttributeType
    case object ProvidedOrders extends AttributeType
    case object LeveragedOrders extends AttributeType
    case object StableLeafPlans extends AttributeType
  }
}
