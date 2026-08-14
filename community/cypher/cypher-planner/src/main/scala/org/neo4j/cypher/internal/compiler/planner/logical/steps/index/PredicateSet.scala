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

import org.neo4j.cypher.internal.ast.IrHint
import org.neo4j.cypher.internal.ast.UsingIndexHint
import org.neo4j.cypher.internal.ast.UsingIndexHint.UsingAnyIndexType
import org.neo4j.cypher.internal.ast.UsingIndexHint.UsingPointIndexType
import org.neo4j.cypher.internal.ast.UsingIndexHint.UsingRangeIndexType
import org.neo4j.cypher.internal.ast.UsingIndexHint.UsingTextIndexType
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.expressions.EntityType
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelOrRelTypeName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.PropertyKeyToken
import org.neo4j.cypher.internal.logical.plans.GetValueFromIndexBehavior
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor.IndexType
import org.neo4j.cypher.internal.util.SymbolicName
import org.neo4j.cypher.internal.util.collection.immutable.ListSet

/**
 * Information needed to create a index leaf plan.
 */
trait PredicateSet {
  def variable: LogicalVariable
  def symbolicName: SymbolicName
  def propertyPredicates: Seq[IndexCompatiblePredicateWithValueBehavior]

  def getEntityType: EntityType

  def allSolvedPredicates: Seq[Expression] =
    propertyPredicates.flatMap(_.indexCompatiblePredicate.solvedPredicate)

  def indexedProperties(context: LogicalPlanningContext): Seq[IndexedProperty] =
    propertyPredicates.map {
      case IndexCompatiblePredicateWithValueBehavior(
          predicate: IndexCompatiblePredicate,
          behavior: GetValueFromIndexBehavior
        ) =>
        val propertyName = predicate.propertyKeyName
        val getValue = behavior
        IndexedProperty(
          PropertyKeyToken(propertyName, context.semanticTable.id(propertyName).head),
          getValue,
          getEntityType
        )
    }

  private def matchingHints(hints: ListSet[IrHint]): Set[UsingIndexHint] = {
    val propertyNames = propertyPredicates.map(_.indexCompatiblePredicate.propertyKeyName.name)
    val localVariableName = variable
    val entityTypeName = symbolicName.name
    hints.collect {
      case hint @ UsingIndexHint(
          `localVariableName`,
          LabelOrRelTypeName(`entityTypeName`),
          propertyKeyNames,
          _,
          _
        ) if propertyKeyNames.map(_.name) == propertyNames => hint
    }
  }

  private def fulfilledByIndexType(indexType: IndexType)(hint: UsingIndexHint): Boolean =
    (hint.indexType, indexType) match {
      case (UsingAnyIndexType, _)                 => true
      case (UsingTextIndexType, IndexType.Text)   => true
      case (UsingRangeIndexType, IndexType.Range) => true
      case (UsingPointIndexType, IndexType.Point) => true
      case _                                      => false
    }

  def fulfilledHints(allHints: ListSet[IrHint], indexType: IndexType, planIsScan: Boolean): Set[UsingIndexHint] =
    matchingHints(allHints)
      .filter(fulfilledByIndexType(indexType))
      .filter(!planIsScan || _.spec.fulfilledByScan)
}
