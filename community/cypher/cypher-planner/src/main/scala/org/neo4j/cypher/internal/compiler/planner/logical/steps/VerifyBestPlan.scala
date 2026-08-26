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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.common.EntityType
import org.neo4j.cypher.internal.ast.IrHint
import org.neo4j.cypher.internal.ast.LeafPlanHint
import org.neo4j.cypher.internal.ast.UsingIndexHint
import org.neo4j.cypher.internal.ast.UsingIndexHint.UsingAnyIndexType
import org.neo4j.cypher.internal.ast.UsingIndexHint.UsingIndexHintType
import org.neo4j.cypher.internal.ast.UsingIndexHint.UsingPointIndexType
import org.neo4j.cypher.internal.ast.UsingIndexHint.UsingRangeIndexType
import org.neo4j.cypher.internal.ast.UsingIndexHint.UsingTextIndexType
import org.neo4j.cypher.internal.ast.UsingJoinHint
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.steps.QuerySolvableByGetDegree.SetExtractor
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.IndexCompatiblePredicate
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.IndexCompatiblePredicatesProvider
import org.neo4j.cypher.internal.expressions.LabelOrRelTypeName
import org.neo4j.cypher.internal.expressions.LogicalProperty
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.notification.IndexHintUnfulfillableNotification
import org.neo4j.cypher.internal.notification.JoinHintUnfulfillableNotification
import org.neo4j.cypher.internal.util.collection.immutable.ListSet
import org.neo4j.cypher.internal.util.helpers.NameDeduplicator.removeGeneratedNamesAndParams
import org.neo4j.cypher.internal.util.helpers.NameDeduplicator.removeGeneratedNamesAndParamsOnTree
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.exceptions.HintException
import org.neo4j.exceptions.IndexHintException
import org.neo4j.exceptions.IndexHintException.IndexHintIndexType
import org.neo4j.exceptions.InternalException
import org.neo4j.exceptions.InvalidHintException
import org.neo4j.exceptions.JoinHintException
import org.neo4j.exceptions.Neo4jException
import org.neo4j.exceptions.UnfulfillableHintException
import org.neo4j.messages.MessageUtil
import org.neo4j.messages.MessageUtil.Numerus

import scala.jdk.CollectionConverters.SeqHasAsJava

object VerifyBestPlan {
  private val prettifier = Prettifier(ExpressionStringifier())

  def apply(plan: LogicalPlan, expected: PlannerQuery, context: LogicalPlanningContext): Unit = {
    val constructed: PlannerQuery = context.staticComponents.planningAttributes.solveds.get(plan.id)

    if (expected != constructed) {
      val expectedSolved: PlannerQuery = expected.withoutHints(expected.allHints)
      val actualSolved: PlannerQuery = constructed.withoutHints(constructed.allHints).withoutImpliedExpressions
      if (expectedSolved != actualSolved) {
        val expectedTitle = "Expected"
        val actualTitle = "Actual"
        // case: unknown planner issue failed to find plan (without regard for differences in hints)
        val moreDetails =
          (expectedSolved, actualSolved) match {
            case (expectedSingle: RegularSinglePlannerQuery, constructedSolved: RegularSinglePlannerQuery) =>
              expectedSingle.pointOutDifference(constructedSolved, expectedTitle, actualTitle)
            case _ => ""
          }

        throw InternalException.internalError(
          this.getClass.getSimpleName,
          s"""$expectedTitle:
             |$expectedSolved
             |
             |$actualTitle:
             |$actualSolved
             |
             |Plan:
             |$plan
             |
             |Verbose plan:
             |${plan.verboseToString}
             |
             |$moreDetails""".stripMargin
        )
      } else {
        // Case: We did construct a plan that suffices the PlannerQuery requested.
        // However, something went wrong with the hints... Let's analyse.

        def prettify(h: ListSet[IrHint]): String =
          h.map(prettifier.asString).mkString("`", ", ", "`")

        def throwHintException(details: String): Nothing = {
          val message =
            s"""Failed to fulfil the hints of the query.
               |$details
               |
               |Plan $plan""".stripMargin

          throw HintException.internalError(this.getClass.getSimpleName, message)
        }

        analyseHints(expected, constructed, context) match {
          case HintAnalysis(missingUnfulfillable, SetExtractor(), SetExtractor()) =>
            // case: the missing hints were unfulfillable
            processUnfulfilledIndexOrScanHints(context, missingUnfulfillable.unfulfillableIndexOrScanHints)
            processUnfulfilledJoinHints(plan, context, missingUnfulfillable.unfulfillableJoinHints)

          case HintAnalysis(missingUnfulfillableHints, SetExtractor(), _)
            if missingUnfulfillableHints.isEmpty =>
            // case: the planner came up with hints that were not in the original query
            val details = s"""Expected hints:
                             |${prettify(expected.allHints)}
                             |
                             |Instead, got:
                             |${prettify(constructed.allHints)}""".stripMargin
            throwHintException(details)

          case HintAnalysis(missingUnfulfillable, missing, _) =>
            // case: we did not fulfill all hints from the original query
            val details = s"Could not solve these hints: ${prettify(missingUnfulfillable.hints ++ missing)}"
            throwHintException(details)
        }
      }
    }
  }

  /**
   * These hints could not have been detected as unfulfillable during semantic analysis.
   */
  private case class UnfulfillableHints(
    unfulfillableIndexOrScanHints: UnfulfillableIndexOrScanHints,
    unfulfillableJoinHints: ListSet[UsingJoinHint]
  ) {
    def hints: ListSet[IrHint] = unfulfillableJoinHints ++ unfulfillableIndexOrScanHints.hints

    def isEmpty: Boolean = hints.isEmpty
  }

  /**
   * The result of the hint analysis, dividing hint differences into three categories.
   * While the first category of unfulfillable hints indicates that some index was missing or similar, the other two categories indicate an error in the planner.
   * 
   * @param missingUnfulfillableHints hints missing from the plan that from a syntax perspective could have been fulfilled
   * @param missingHints other hints missing from the plan
   * @param inventedHints hints on the constructed plan that were not part of the original query
   */
  private case class HintAnalysis(
    missingUnfulfillableHints: UnfulfillableHints,
    missingHints: ListSet[IrHint],
    inventedHints: ListSet[IrHint]
  )

  /**
   * Analyse, in how far the two queries (read: IR) differ in their hints.
   * 
   * @param expected the query we generated from the original Cypher query
   * @param constructed the query we generated from the plan
   */
  private def analyseHints(
    expected: PlannerQuery,
    constructed: PlannerQuery,
    context: LogicalPlanningContext
  ): HintAnalysis = {
    val expectedHints = expected.allHints
    val actualHints = constructed.allHints
    val missingHints = expectedHints.diff(actualHints)
    val inventedHints = actualHints.diff(expectedHints)

    val missingUnfulfillableIndexOrScanHints = findUnfulfillableIndexOrScanHints(expected, context).filter(missingHints)
    val missingUnfulfillableJoinHints =
      missingHints
        .collect {
          // as a rough measure, we consider all join hints as unfulfillable.
          // That is, that we cannot judge from the syntax alone whether these hints can be fulfilled, as this highly depends on the planner's inner workings
          case hint: UsingJoinHint => hint
        }
    val missingUnfulfillableHints =
      UnfulfillableHints(missingUnfulfillableIndexOrScanHints, missingUnfulfillableJoinHints)

    val missingFulfillableHints = missingHints.filterNot(missingUnfulfillableHints.hints)

    HintAnalysis(missingUnfulfillableHints, missingFulfillableHints, inventedHints)
  }

  private def processUnfulfilledIndexOrScanHints(
    context: LogicalPlanningContext,
    unfulfillableIndexHints: UnfulfillableIndexOrScanHints
  ): Unit = {
    unfulfillableIndexHints.wrongPropertyTypeHints.headOption.foreach {
      (wrongHint: WrongPropertyTypeHint) =>
        val entityType = context.semanticTable.typeFor(wrongHint.hint.variable)
        val entity = if (entityType.is(CTNode)) "NODE" else "RELATIONSHIP"

        throw wrongHint.toException(entity)
    }
    val hints = unfulfillableIndexHints.missingIndexHints
    if (hints.nonEmpty) {
      // hints referred to non-existent indexes ("explicit hints")
      if (context.settings.useErrorsOverWarnings) {
        throw hints.head.toException
      } else {
        hints.foreach { hint =>
          context.staticComponents.notificationLogger.log(hint.toNotification)
        }
      }
    }
    val duplicateHints = unfulfillableIndexHints.duplicateLeafPlanHints
    if (duplicateHints.nonEmpty) {
      throw DuplicateLeafPlanHints.toException(duplicateHints)
    }

    unfulfillableIndexHints.alreadyBoundVariableHints.headOption.foreach {
      (alreadyBoundVariableHint: AlreadyBoundVariableHint) => throw alreadyBoundVariableHint.toException
    }
  }

  private def processUnfulfilledJoinHints(
    plan: LogicalPlan,
    context: LogicalPlanningContext,
    hints: ListSet[UsingJoinHint]
  ): Unit = {
    if (hints.nonEmpty) {
      // we were unable to plan hash join on some requested nodes
      if (context.settings.useErrorsOverWarnings) {
        throw JoinHintException.unableToPlanHashJoin(hints.map(prettifier.asString).toSeq.asJava, plan.toString)
      } else {
        hints.foreach { hint =>
          context.staticComponents.notificationLogger.log(
            JoinHintUnfulfillableNotification(hint.variables.map(_.name).toIndexedSeq)
          )
        }
      }
    }
  }

  /**
   * Index hint to which no index exists.
   * @param hint the original hint
   * @param entityType the type of the entity this hint refers to
   */
  private case class MissingIndexHint(hint: UsingIndexHint, entityType: EntityType) {

    def toNotification: IndexHintUnfulfillableNotification =
      IndexHintUnfulfillableNotification(
        hint.variable.name,
        hint.labelOrRelType.name,
        hint.properties.map(_.name),
        entityType,
        MissingIndexHint.toIndexHintIndexType(hint.indexType)
      )

    def toException: IndexHintException = {
      val exceptionIndexType = MissingIndexHint.toIndexHintIndexType(hint.indexType)
      val formattedIndex = IndexHintException.indexFormatString(
        hint.variable.name,
        hint.labelOrRelType.name,
        hint.properties.map(_.name).asJava,
        entityType,
        exceptionIndexType
      )
      IndexHintException.indexNotFound(
        hint.variable.name,
        hint.labelOrRelType.name,
        hint.properties.map(_.name).asJava,
        entityType,
        exceptionIndexType,
        formattedIndex
      )
    }
  }

  private object MissingIndexHint {

    private def toIndexHintIndexType(indexType: UsingIndexHintType): IndexHintIndexType = indexType match {
      case UsingAnyIndexType   => IndexHintIndexType.ANY
      case UsingTextIndexType  => IndexHintIndexType.TEXT
      case UsingRangeIndexType => IndexHintIndexType.RANGE
      case UsingPointIndexType => IndexHintIndexType.POINT
    }
  }

  /**
   * The given hint cannot be fulfilled because the properties used for this hint are not of the correct type.
   * @param hint the offending hint
   */
  private case class WrongPropertyTypeHint(hint: UsingIndexHint, foundPredicates: Set[IndexCompatiblePredicate]) {

    def toException(entity: String): Neo4jException = {
      val prettifiedHint = prettifier.asString(hint)
      val legacyMessage = MessageUtil.createTextIndexHintError(
        prettifiedHint,
        Numerus.of(foundPredicates.size)
      )
      InvalidHintException.cannotUseTextIndexHint(legacyMessage, prettifiedHint, entity, hint.variable.name)
    }
  }

  /**
   * Several leaf plan hints on the same variable within the same query graph. Only one of them can ever be planned.
   *
   * For example: `MATCH (n:Person) USING INDEX n:Person(name) USING INDEX n:Person(surname) WHERE ...`
   *
   * @param variable the variable all these hints refer to
   * @param competingHints all the competing hints
   * @param hints the subset of `competingHints` that the planner did not manage to fulfil
   */
  private case class DuplicateLeafPlanHints(
    variable: Variable,
    competingHints: Seq[LeafPlanHint],
    hints: Seq[LeafPlanHint]
  ) {

    def filter(predicate: IrHint => Boolean): DuplicateLeafPlanHints = copy(hints = hints.filter(predicate))
  }

  private object DuplicateLeafPlanHints {

    def apply(variable: Variable, competingHints: Seq[LeafPlanHint]): DuplicateLeafPlanHints =
      DuplicateLeafPlanHints(variable, competingHints, competingHints)

    /**
     * Report all conflicting variables in a single exception, listing all the hints that compete with each other.
     */
    def toException(duplicates: Seq[DuplicateLeafPlanHints]): Neo4jException =
      UnfulfillableHintException.duplicateHints(
        duplicates.flatMap(_.competingHints).sortBy(_.position.offset).map(prettifyHint).asJava,
        duplicates.map(duplicate => prettifyVariable(duplicate.variable)).distinct.asJava
      )
  }

  /**
   * A leaf plan hint on a variable that is already bound when its query graph is planned, so no leaf plan can be
   * chosen for it.
   *
   * For example: `MATCH (n) SKIP 0 MATCH (n:Person) USING SCAN n:Person`
   *
   * @param hint the offending hint
   */
  private case class AlreadyBoundVariableHint(hint: LeafPlanHint) {

    def toException: Neo4jException = {
      UnfulfillableHintException.alreadyBoundVariableHint(
        prettifyHint(hint),
        removeGeneratedNamesAndParams(hint.variable.name)
      )
    }
  }

  private case class UnfulfillableIndexOrScanHints(
    missingIndexHints: Set[MissingIndexHint],
    wrongPropertyTypeHints: collection.Seq[WrongPropertyTypeHint],
    duplicateLeafPlanHints: Seq[DuplicateLeafPlanHints],
    alreadyBoundVariableHints: Seq[AlreadyBoundVariableHint]
  ) {

    val hints: ListSet[IrHint] =
      ListSet.empty[IrHint] ++ missingIndexHints.map(_.hint) ++ wrongPropertyTypeHints.map(
        _.hint
      ) ++ duplicateLeafPlanHints.flatMap(_.hints) ++ alreadyBoundVariableHints.map(_.hint)

    def filter(predicate: IrHint => Boolean): UnfulfillableIndexOrScanHints =
      UnfulfillableIndexOrScanHints(
        missingIndexHints =
          missingIndexHints
            .filter(hint => predicate(hint.hint)),
        wrongPropertyTypeHints =
          wrongPropertyTypeHints
            .filter(hint => predicate(hint.hint)),
        duplicateLeafPlanHints =
          duplicateLeafPlanHints
            .map(_.filter(predicate))
            .filter(_.hints.nonEmpty),
        alreadyBoundVariableHints =
          alreadyBoundVariableHints
            .filter(hint => predicate(hint.hint))
      )
  }

  private def findUnfulfillableIndexOrScanHints(
    query: PlannerQuery,
    context: LogicalPlanningContext
  ): UnfulfillableIndexOrScanHints = {
    val planContext = context.staticComponents.planContext
    val semanticTable = context.semanticTable

    def nodeIndexForHintExists(
      labelOrRelType: LabelOrRelTypeName,
      properties: Seq[PropertyKeyName],
      indexHintType: UsingIndexHintType
    ): Boolean = {
      val labelName = labelOrRelType.name
      val propertyNames = properties.map(_.name)

      lazy val textExists = planContext.textIndexExistsForLabelAndProperties(labelName, propertyNames)
      lazy val rangeExists = planContext.rangeIndexExistsForLabelAndProperties(labelName, propertyNames)
      lazy val pointExists = planContext.pointIndexExistsForLabelAndProperties(labelName, propertyNames)

      indexHintType match {
        case UsingAnyIndexType   => textExists || rangeExists || pointExists
        case UsingTextIndexType  => textExists
        case UsingRangeIndexType => rangeExists
        case UsingPointIndexType => pointExists
      }
    }

    def relIndexForHintExists(
      labelOrRelType: LabelOrRelTypeName,
      properties: Seq[PropertyKeyName],
      indexHintType: UsingIndexHintType
    ): Boolean = {
      val relTypeName = labelOrRelType.name
      val propertyNames = properties.map(_.name)

      lazy val textExists = planContext.textIndexExistsForRelTypeAndProperties(relTypeName, propertyNames)
      lazy val rangeExists = planContext.rangeIndexExistsForRelTypeAndProperties(relTypeName, propertyNames)
      lazy val pointExists = planContext.pointIndexExistsForRelTypeAndProperties(relTypeName, propertyNames)

      indexHintType match {
        case UsingAnyIndexType   => textExists || rangeExists || pointExists
        case UsingTextIndexType  => textExists
        case UsingRangeIndexType => rangeExists
        case UsingPointIndexType => pointExists
      }
    }

    val hintsForWrongType = collectWrongPropertyTypeHints(query, semanticTable)

    val hintsWithoutIndex = query.allHints.flatMap {
      // using index name:label(property1,property2)
      case UsingIndexHint(v, labelOrRelType, properties, _, indexHintType)
        if semanticTable.typeFor(v.name).is(CTNode) && nodeIndexForHintExists(
          labelOrRelType,
          properties,
          indexHintType
        ) =>
        None

      // using index name:relType(property1,property2)
      case UsingIndexHint(v, labelOrRelType, properties, _, indexHintType)
        if semanticTable.typeFor(v.name).is(CTRelationship) && relIndexForHintExists(
          labelOrRelType,
          properties,
          indexHintType
        ) =>
        None

      // no such index exists
      case hint: UsingIndexHint =>
        // Let's assume node type by default, in case we have no type information.
        val entityType =
          if (semanticTable.typeFor(hint.variable).is(CTRelationship)) EntityType.RELATIONSHIP else EntityType.NODE
        Some(MissingIndexHint(hint, entityType))
      // don't care about other hints
      case _ => None
    }

    UnfulfillableIndexOrScanHints(
      hintsWithoutIndex,
      hintsForWrongType.toVector,
      collectDuplicateLeafPlanHints(query),
      collectAlreadyBoundVariableHints(query)
    )
  }

  /**
   * Find, per query graph, the variables that carry more than one leaf plan hint.
   *
   * Grouping has to happen per query graph rather than per query: hints on the same variable in different query
   * parts do not compete with each other.
   */
  private def collectDuplicateLeafPlanHints(query: PlannerQuery): Seq[DuplicateLeafPlanHints] = {
    def duplicatesIn(queryGraph: QueryGraph): Seq[DuplicateLeafPlanHints] = {
      val leafPlanHints = queryGraph.hints.toSeq.collect { case hint: LeafPlanHint => hint }
      val hintsByVariable = leafPlanHints.groupBy(_.variable)
      leafPlanHints
        .map(_.variable)
        .distinct
        .map(variable => DuplicateLeafPlanHints(variable, hintsByVariable(variable)))
        .filter(element =>
          element.hints.size > 1 && element.competingHints.exists(_.isInstanceOf[UsingIndexHint])
        )
    }

    query
      .visitHints(ListSet.empty[QueryGraph]) { case (queryGraphs, _, queryGraph) => queryGraphs + queryGraph }
      .toSeq
      .flatMap(duplicatesIn)
  }

  private def collectAlreadyBoundVariableHints(query: PlannerQuery): Seq[AlreadyBoundVariableHint] = {
    query.visitHints(Vector.empty[AlreadyBoundVariableHint]) {
      case (acc, hint: LeafPlanHint, queryGraph) if queryGraph.argumentIds.contains(hint.variable) =>
        acc :+ AlreadyBoundVariableHint(hint)
      case (acc, _, _) => acc
    }
  }

  private def collectWrongPropertyTypeHints(
    query: PlannerQuery,
    semanticTable: SemanticTable
  ): Set[WrongPropertyTypeHint] = {
    query.visitHints(Set.empty[WrongPropertyTypeHint]) {
      case (acc, hint @ UsingIndexHint(variable, _, Seq(property), _, UsingTextIndexType), queryGraph) =>
        acc ++ hasPropertyOfTypeText(variable, property, semanticTable, queryGraph).left.toOption.map(
          WrongPropertyTypeHint(hint, _)
        )
      case (acc, _, _) => acc
    }
  }

  /**
   * Tests whether there exists a predicate on the given property that can be used by a text index. And if not, return the predicates searched through.
   */
  private def hasPropertyOfTypeText(
    variable: Variable,
    propertyName: PropertyKeyName,
    semanticTable: SemanticTable,
    queryGraph: QueryGraph
  ): Either[Set[IndexCompatiblePredicate], Boolean] = {
    val predicates = queryGraph.selections.flatPredicates.toSet
    val arguments: Set[LogicalVariable] = queryGraph.argumentIds
    val matchingPredicates =
      IndexCompatiblePredicatesProvider.findExplicitCompatiblePredicates(arguments, predicates, semanticTable).collect {
        case pred @ IndexCompatiblePredicate(`variable`, LogicalProperty(_, `propertyName`), _, _, _, _, _, _, _, _) =>
          pred
      }
    if (matchingPredicates.exists(_.cypherType.isSubtypeOf(CTString))) {
      Right(true)
    } else {
      Left(matchingPredicates)
    }
  }

  /**
   * Variables may have been renamed by the planner, for example to `  n@0` when namespacing a UNION query.
   * Such generated names should not leak into user facing messages.
   */
  private def prettifyHint(hint: IrHint): String = prettifier.asString(removeGeneratedNamesAndParamsOnTree(hint))

  private def prettifyVariable(variable: Variable): String =
    prettifier.expr(removeGeneratedNamesAndParamsOnTree(variable))
}
