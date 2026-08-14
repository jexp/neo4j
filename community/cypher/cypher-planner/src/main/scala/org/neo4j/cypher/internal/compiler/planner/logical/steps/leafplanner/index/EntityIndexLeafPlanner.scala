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
package org.neo4j.cypher.internal.compiler.planner.logical.steps.leafplanner.index

import org.neo4j.cypher.internal.ast.semantics.TokenTable
import org.neo4j.cypher.internal.compiler.helpers.PropertyAccessHelper.PropertyAccess
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.ResultOrdering
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.ResultOrdering.PropertyAndPredicateType
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.IndexCompatiblePredicate
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.IndexRequirement
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.NotExactPredicate
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.SingleExactPredicate
import org.neo4j.cypher.internal.expressions.IsNotNull
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.frontend.helpers.SeqCombiner
import org.neo4j.cypher.internal.logical.plans.ExistenceQueryExpression
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.logical.plans.ordering.ProvidedOrder
import org.neo4j.cypher.internal.logical.plans.ordering.ProvidedOrderFactory
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTPoint
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.internal.schema.IndexQuery.IndexQueryType
import org.neo4j.internal.schema.constraints.ConstrainableType
import org.neo4j.internal.schema.constraints.SchemaValueType

/**
 * Common functionality of NodeIndexLeafPlanner and RelationshipIndexLeafPlanner.
 */
object EntityIndexLeafPlanner {

  /**
   * Creates IS NOT NULL-predicates of the given variable to the given properties that are inferred from the context rather than read from the query.
   */
  private[index] def implicitIsNotNullPredicates(
    variable: LogicalVariable,
    aggregatingProperties: Set[PropertyAccess],
    constrainedPropNames: Set[String],
    explicitCompatiblePredicates: Set[IndexCompatiblePredicate]
  ): Set[IndexCompatiblePredicate] = {
    // Can't currently handle aggregation on more than one variable
    val aggregatedPropNames: Set[String] =
      if (aggregatingProperties.forall(prop => prop.variable.equals(variable))) {
        aggregatingProperties.map { prop => prop.propertyName }
      } else {
        Set.empty
      }

    //  Can't currently handle aggregation on more than one property
    val propNames =
      if (aggregatedPropNames.size == 1) constrainedPropNames.union(aggregatedPropNames) else constrainedPropNames

    for {
      propertyName <- propNames
      property = Property(variable, PropertyKeyName(propertyName)(variable.position))(variable.position)
      predicate = IsNotNull(property)(variable.position)
      // Don't add implicit predicates if we already have them explicitly
      if !explicitCompatiblePredicates.exists(_.predicate == predicate)
    } yield IndexCompatiblePredicate(
      variable,
      property,
      predicate,
      ExistenceQueryExpression,
      predicateExactness = NotExactPredicate,
      solvedPredicate = None,
      dependencies = Set.empty,
      isImplicit = true,
      indexRequirements = Set(IndexRequirement.SupportsIndexQuery(IndexQueryType.EXISTS)),
      cypherType = CTAny
    )
  }

  /**
   * If a type constraint exists for the property of a predicate, we may sometimes also consider a scannable version of the predicate.
   * For example, if a string type constraint exists on n.prop, then any scannable predicate on n.prop is solvable by a text index.
   */
  private def resolveConstraintTypes(
    predicate: IndexCompatiblePredicate,
    typeConstraints: Map[String, Seq[ConstrainableType]]
  ): Seq[IndexCompatiblePredicate] = {
    typeConstraints.get(predicate.propertyKeyName.name) match {
      case Some(Seq(SchemaValueType.STRING)) =>
        Seq(
          predicate,
          predicate.copy(cypherType = CTString),
          predicate.convertToTextScannable.copy(cypherType = CTString)
        )
      case Some(Seq(SchemaValueType.POINT)) =>
        Seq(
          predicate,
          predicate.copy(cypherType = CTPoint),
          predicate.convertToPointScannable.copy(cypherType = CTPoint)
        )
      case _ => Seq(predicate)
    }
  }

  /**
   * Find and group all predicates, where one PredicatesForIndex contains one predicate for each indexed property, in the right order.
   */
  private[index] def predicatesForIndex(
    indexDescriptor: IndexDescriptor,
    predicates: Set[IndexCompatiblePredicate],
    interestingOrderConfig: InterestingOrderConfig,
    tokenTable: TokenTable,
    typeConstraints: Map[String, Seq[ConstrainableType]],
    providedOrderFactory: ProvidedOrderFactory
  ): Set[PredicatesForIndex] = {

    // Group predicates by which property they include
    val predicatesByProperty = predicates
      .flatMap(resolveConstraintTypes(_, typeConstraints))
      .filter(predicate => predicate.indexRequirements.forall(req => req.satisfiedBy(indexDescriptor, predicate)))
      .groupBy(icp => tokenTable.id(icp.propertyKeyName))
      // Sort out predicates that are not found in semantic table
      .collect { case (Some(x), v) => (x, v) }

    // For each indexed property, look up the relevant predicates
    val predicatesByIndexedProperty = indexDescriptor.properties
      .map(indexedProp => predicatesByProperty.getOrElse(indexedProp, Set.empty))

    // All combinations of predicates where each inner Seq covers the indexed properties in the correct order.
    // E.g. for an index on foo, bar and the predicates predFoo1, predFoo2, predBar1, this would return
    // Seq(Seq(predFoo1, predBar1), Seq(predFoo2, predBar1)).
    val matchingPredicateCombinations = SeqCombiner.combine(predicatesByIndexedProperty).toSet

    matchingPredicateCombinations
      .map(matchingPredicates =>
        matchPredicateWithIndexDescriptorAndInterestingOrder(
          matchingPredicates,
          indexDescriptor,
          interestingOrderConfig,
          providedOrderFactory
        )
      )
  }

  private def matchPredicateWithIndexDescriptorAndInterestingOrder(
    matchingPredicates: Seq[IndexCompatiblePredicate],
    indexDescriptor: IndexDescriptor,
    interestingOrderConfig: InterestingOrderConfig,
    providedOrderFactory: ProvidedOrderFactory
  ): PredicatesForIndex = {

    // Ask the index for its order capability
    val indexPropertiesAndPredicateTypes = matchingPredicates.map(mp => {
      val property = Property(mp.variable, mp.propertyKeyName)(mp.property.position)
      PropertyAndPredicateType(property, mp.predicateExactness == SingleExactPredicate)
    })

    val (providedOrder, indexOrder) =
      ResultOrdering.providedOrderForIndexOperator(
        interestingOrderConfig.orderToSolve,
        indexPropertiesAndPredicateTypes,
        indexDescriptor.orderCapability,
        providedOrderFactory
      )

    PredicatesForIndex(matchingPredicates, providedOrder, indexOrder)
  }

  private[index] case class PredicatesForIndex(
    predicatesInOrder: Seq[IndexCompatiblePredicate],
    providedOrder: ProvidedOrder,
    indexOrder: IndexOrder
  )

}
