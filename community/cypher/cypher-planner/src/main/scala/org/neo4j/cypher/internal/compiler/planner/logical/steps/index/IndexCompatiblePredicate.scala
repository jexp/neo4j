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

import org.neo4j.cypher.internal.ast.IsTyped
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsExplicitlyPropertyScannable
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.IsNotNull
import org.neo4j.cypher.internal.expressions.LogicalProperty
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.PartialPredicate
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.logical.plans.CompositeQueryExpression
import org.neo4j.cypher.internal.logical.plans.ExistenceQueryExpression
import org.neo4j.cypher.internal.logical.plans.QueryExpression
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor.IndexType
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTPoint
import org.neo4j.cypher.internal.util.symbols.CTPointNotNull
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.CTStringNotNull
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.internal.schema.IndexQuery.IndexQueryType

/**
 * A predicate that could potentially be solved by a property index
 *
 * @param variable             The variable to solve for
 * @param property             The property involved in the predicate
 * @param predicate            The original predicate from the query
 * @param queryExpression      The index query expression
 * @param predicateExactness   Determines seek possibility
 * @param solvedPredicate      If a plan is created, this is what to register as solved predicate
 * @param dependencies         Predicate dependencies
 * @param isImplicit           if `true` then the predicate is not explicitly stated in the query
 * @param indexRequirements    Requirements an index must satisfy to be considered a match for this predicate
 * @param cypherType           the type of the property
 */
case class IndexCompatiblePredicate(
  variable: LogicalVariable,
  property: LogicalProperty,
  predicate: Expression,
  queryExpression: QueryExpression[Expression],
  predicateExactness: PredicateExactness,
  solvedPredicate: Option[Expression],
  dependencies: Set[LogicalVariable],
  isImplicit: Boolean = false,
  indexRequirements: Set[IndexRequirement],
  cypherType: CypherType
) {
  def propertyKeyName: PropertyKeyName = property.propertyKey

  def isExists: Boolean = queryExpression match {
    case ExistenceQueryExpression => true
    case _                        => false
  }

  def convertToRangeScannable: IndexCompatiblePredicate = queryExpression match {
    case _: CompositeQueryExpression[?] =>
      throw new IllegalStateException("A CompositeQueryExpression can't be nested in a CompositeQueryExpression")

    case _ => copy(
        queryExpression = ExistenceQueryExpression,
        predicateExactness = NotExactPredicate,
        solvedPredicate = solvedPredicate.map(convertToRangeScannablePredicate),
        indexRequirements = Set(IndexRequirement.SupportsIndexQuery(IndexQueryType.EXISTS)),
        cypherType = CTAny
      )
  }

  private def convertToRangeScannablePredicate(expr: Expression): Expression = {
    val original = PartialPredicate.unwrap(expr)
    original match {
      case AsExplicitlyPropertyScannable(scannable) =>
        scannable.expr
      case partialExpr =>
        val isNotNull = IsNotNull(property)(predicate.position)
        PartialPredicate(isNotNull, partialExpr)
    }
  }

  def convertToTextScannable: IndexCompatiblePredicate = {
    copy(
      queryExpression = ExistenceQueryExpression,
      predicateExactness = NotExactPredicate,
      solvedPredicate = solvedPredicate.map(convertToTextScannablePredicate),
      indexRequirements = Set(
        IndexRequirement.SupportsIndexQuery(IndexQueryType.ALL_ENTRIES),
        IndexRequirement.HasType(IndexType.Text)
      )
    )
  }

  private def convertToTextScannablePredicate(expr: Expression): Expression = {
    PartialPredicate.unwrap(expr) match {
      case AsExplicitlyPropertyScannable(scannable) if cypherType == CTString => scannable.expr
      case expr => PartialPredicate(
          IsTyped(property, CTStringNotNull)(predicate.position, IsTyped.withDoubleColonOnlyDefault),
          expr
        )
    }
  }

  def convertToPointScannable: IndexCompatiblePredicate = {
    copy(
      queryExpression = ExistenceQueryExpression,
      predicateExactness = NotExactPredicate,
      solvedPredicate = solvedPredicate.map(convertToPointScannablePredicate),
      indexRequirements = Set(
        IndexRequirement.SupportsIndexQuery(IndexQueryType.ALL_ENTRIES),
        IndexRequirement.HasType(IndexType.Point)
      )
    )
  }

  private def convertToPointScannablePredicate(expr: Expression): Expression = {
    PartialPredicate.unwrap(expr) match {
      case AsExplicitlyPropertyScannable(scannable) if cypherType == CTPoint => scannable.expr
      case expr => PartialPredicate(
          IsTyped(property, CTPointNotNull)(predicate.position, IsTyped.withDoubleColonOnlyDefault),
          expr
        )
    }
  }
}
