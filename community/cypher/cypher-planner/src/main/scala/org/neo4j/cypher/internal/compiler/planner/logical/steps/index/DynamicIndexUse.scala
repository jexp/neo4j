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
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.PropertyKeyToken
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.logical.plans.SingleQueryExpression

/**
 * Utilities for planning index use with dynamic labels/relationship types.
 */
object DynamicIndexUse {

  case class PropertyPredicates(
    solvedPredicates: Set[Expression],
    indexSeekArguments: Map[PropertyKeyToken, Expression]
  )

  private case class PropertyPredicate(solvedPredicate: Expression, indexSeekArgument: Expression)

  class PropertyPredicatesHelper(queryGraph: QueryGraph, context: LogicalPlanningContext) {

    def predicatesForVariable(v: LogicalVariable): PropertyPredicates = {
      if (!context.settings.dynamicLabelIndexUseEnabled) {
        PropertyPredicates(Set.empty, Map.empty)
      } else {
        val predMap = predicates.getOrElse(v, Map.empty)
        PropertyPredicates(
          solvedPredicates = predMap.values.map(_.solvedPredicate).toSet,
          indexSeekArguments = predMap.view.mapValues(_.indexSeekArgument).toMap
        )
      }
    }

    private lazy val predicates: Map[LogicalVariable, Map[PropertyKeyToken, PropertyPredicate]] = {
      IndexCompatiblePredicatesProvider.findExplicitCompatiblePredicates(
        queryGraph.argumentIds,
        queryGraph.selections.flatPredicatesSet,
        context.semanticTable
      ).groupBy(_.variable).view.mapValues(icps =>
        icps.collect {
          case IndexCompatiblePredicate(
              _,
              property,
              originalPredicate,
              SingleQueryExpression(indexSeekArgument),
              SingleExactPredicate,
              solvedPredicate,
              _,
              isImplicit,
              _,
              _
            )
            if !isImplicit && context.semanticTable.id(property.propertyKey).isDefined =>

            val key = PropertyKeyToken(property.propertyKey, context.semanticTable.id(property.propertyKey).get)
            val value = PropertyPredicate(
              solvedPredicate = solvedPredicate.getOrElse(originalPredicate),
              indexSeekArgument = indexSeekArgument
            )
            key -> value
        }.groupMapReduce(_._1)(_._2)((v1, v2) => Seq(v1, v2).minBy(_.solvedPredicate.position))
      ).filter(_._2.nonEmpty).toMap
    }
  }
}
