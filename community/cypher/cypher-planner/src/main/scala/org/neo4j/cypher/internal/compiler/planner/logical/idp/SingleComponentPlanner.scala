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
package org.neo4j.cypher.internal.compiler.planner.logical.idp

import org.neo4j.cypher.internal.compiler.planner.logical.InterestingOrderSelectorHeuristic
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.QueryPlannerKit
import org.neo4j.cypher.internal.compiler.planner.logical.SortPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.idp.IDPQueryGraphSolver.extraRequirementForInterestingOrder
import org.neo4j.cypher.internal.compiler.planner.logical.idp.SingleComponentPlanner.planSinglePattern
import org.neo4j.cypher.internal.compiler.planner.logical.idp.expandSolverStep.LogicalPlanWithIntoVsAllHeuristic
import org.neo4j.cypher.internal.compiler.planner.logical.idp.expandSolverStep.planSinglePatternSide
import org.neo4j.cypher.internal.compiler.planner.logical.idp.expandSolverStep.planSingleProjectEndpoints
import org.neo4j.cypher.internal.compiler.planner.logical.idp.expandSolverStep.preFilterCandidatesByIntoVsAllHeuristic
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.BestPlans
import org.neo4j.cypher.internal.compiler.planner.logical.steps.leafplanner.LeafPlanFinder
import org.neo4j.cypher.internal.compiler.planner.logical.steps.leafplanner.leafPlanOptions
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.ir.NodeConnection
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.util.collection.immutable.ListSet.IterableOnceToListSet
import org.neo4j.exceptions.InternalException
import org.neo4j.time.Stopwatch

/**
 * This class contains the main IDP loop in the cost planner.
 * This planner is based on the paper
 *
 * "Iterative Dynamic Programming: A New Class of Query Optimization Algorithms"
 *
 * written by Donald Kossmann and Konrad Stocker
 */
case class SingleComponentPlanner(
  solverConfig: SingleComponentIDPSolverConfig = DefaultIDPSolverConfig,
  leafPlanFinder: LeafPlanFinder = leafPlanOptions
)(monitor: IDPQueryGraphSolverMonitor) extends SingleComponentPlannerTrait {

  override def planComponent(
    leafPlanCandidates: Set[LogicalPlan],
    qg: QueryGraph,
    context: LogicalPlanningContext,
    kit: QueryPlannerKit,
    interestingOrderConfig: InterestingOrderConfig
  ): BestPlans = {
    val componentInterestingOrderConfig = interestingOrderConfig.forQueryGraph(qg)
    val bestLeafPlansPerAvailableSymbol =
      leafPlanFinder(leafPlanCandidates, context.plannerState.config, qg, componentInterestingOrderConfig, context)
    planComponent(bestLeafPlansPerAvailableSymbol, qg, context, kit, interestingOrderConfig)
  }

  override def planComponent(
    qg: QueryGraph,
    context: LogicalPlanningContext,
    kit: QueryPlannerKit,
    interestingOrderConfig: InterestingOrderConfig
  ): BestPlans = {
    val componentInterestingOrderConfig = interestingOrderConfig.forQueryGraph(qg)
    val bestLeafPlansPerAvailableSymbol =
      leafPlanFinder(context.plannerState.config, qg, componentInterestingOrderConfig, context)
    planComponent(bestLeafPlansPerAvailableSymbol, qg, context, kit, interestingOrderConfig)
  }

  private def planComponent(
    bestLeafPlansPerAvailableSymbol: Map[Set[LogicalVariable], BestPlans],
    qg: QueryGraph,
    context: LogicalPlanningContext,
    kit: QueryPlannerKit,
    interestingOrderConfig: InterestingOrderConfig
  ): BestPlans = {
    val idpLogger = context.staticComponents.idpLogger
    idpLogger.markScope("planComponent") {
      idpLogger.log(s"component = ${IDPLoggable.summary(qg)}")
      doPlanComponent(bestLeafPlansPerAvailableSymbol, qg, context, kit, interestingOrderConfig)
    }
  }

  private def doPlanComponent(
    bestLeafPlansPerAvailableSymbol: Map[Set[LogicalVariable], BestPlans],
    qg: QueryGraph,
    context: LogicalPlanningContext,
    kit: QueryPlannerKit,
    interestingOrderConfig: InterestingOrderConfig
  ): BestPlans = {
    val componentInterestingOrderConfig = interestingOrderConfig.forQueryGraph(qg)
    val qppInnerPlanner = new CacheBackedQPPInnerPlanner(IDPQPPInnerPlanner(context))

    val bestPlans =
      if (qg.nodeConnections.nonEmpty) {
        val propertyRequirement =
          context.settings.remoteBatchPropertiesStrategy.interestingPropertiesAsIDPExtraRequirement(qg, context)
        val orderRequirement = extraRequirementForInterestingOrder(context, componentInterestingOrderConfig)
        val generators = solverConfig.solvers(qppInnerPlanner).map(_(qg))
        val generator =
          IDPQueryGraphSolver.composeSolverSteps(qg, componentInterestingOrderConfig, kit, context, generators)

        val solver = new IDPSolver[NodeConnection, LogicalPlan, LogicalPlanningContext](
          generator = generator,
          projectingSelector = kit.pickBest,
          maxTableSize = solverConfig.maxTableSize,
          iterationDurationLimit = solverConfig.iterationDurationLimit,
          extraOrderRequirement = orderRequirement,
          extraPropertyRequirement = propertyRequirement,
          monitor = monitor,
          stopWatchFactory = () => Stopwatch.start(),
          cancellationChecker = context.staticComponents.cancellationChecker,
          idpLogger = context.staticComponents.idpLogger
        )

        monitor.initTableFor(qg)
        val seed =
          initTable(
            qg,
            kit,
            bestLeafPlansPerAvailableSymbol,
            qppInnerPlanner,
            context,
            componentInterestingOrderConfig
          )
        monitor.startIDPIterationFor(qg)
        val result = solver(seed, qg.nodeConnections.toListSet, context)
        monitor.endIDPIterationFor(qg, result.bestResult)

        BestResults(result.bestResult, result.bestSortedResult, result.bestExtraPropertiesResult)
      } else {
        val solutionPlans =
          if (qg.shortestRelationshipPatterns.isEmpty && qg.searchClause.isEmpty) {
            bestLeafPlansPerAvailableSymbol
              .values
              .filter(bestPlans => planFullyCoversQG(qg, bestPlans.bestResult))
          } else {
            bestLeafPlansPerAvailableSymbol
              .values
              .map(_.map(kit.select(_, qg)))
              .filter(bestPlans => planFullyCoversQG(qg, bestPlans.bestResult))
          }
        if (solutionPlans.size != 1) {
          throw InternalException.internalError(
            this.getClass.getSimpleName,
            "Found no leaf plan for connected component. This must not happen. QG: " + qg
          )
        }

        val result = solutionPlans.head

        monitor.noIDPIterationFor(qg, result.bestResult)
        result
      }

    if (IDPQueryGraphSolver.VERBOSE) {
      println(
        s"Result (picked best plan):\n\tPlan #${bestPlans.bestResult.debugId}\n\t${bestPlans.bestResult.toString}"
      )
      bestPlans.bestSortedResult.foreach { bSP =>
        println(s"Result (picked best sorted plan):\n\tPlan #${bSP.debugId}\n\t${bSP.toString}")
      }
      bestPlans.bestExtraPropertiesResult.foreach { bestExtraPropPlan =>
        println(
          s"Result (picked best plan with extra properties):\n\tPlan #${bestExtraPropPlan.debugId}\n\t${bestExtraPropPlan.toString}"
        )
      }
      println("\n")
    }
    bestPlans
  }

  private def planFullyCoversQG(qg: QueryGraph, plan: LogicalPlan) =
    (qg.idsWithoutOptionalMatchesOrUpdates -- plan.availableSymbols -- qg.argumentIds).isEmpty

  private def initTable(
    qg: QueryGraph,
    kit: QueryPlannerKit,
    bestLeafPlansPerAvailableSymbol: Map[Set[LogicalVariable], BestPlans],
    qppInnerPlanner: QPPInnerPlanner,
    context: LogicalPlanningContext,
    interestingOrderConfig: InterestingOrderConfig
  ): Seed[NodeConnection, LogicalPlan] = {
    context.staticComponents.idpLogger.markScope("initTable") {
      doInitTable(qg, kit, bestLeafPlansPerAvailableSymbol, qppInnerPlanner, context, interestingOrderConfig)
    }
  }

  private def doInitTable(
    qg: QueryGraph,
    kit: QueryPlannerKit,
    bestLeafPlansPerAvailableSymbol: Map[Set[LogicalVariable], BestPlans],
    qppInnerPlanner: QPPInnerPlanner,
    context: LogicalPlanningContext,
    interestingOrderConfig: InterestingOrderConfig
  ): Seed[NodeConnection, LogicalPlan] = {
    val seed = (for (pattern <- qg.nodeConnections)
      yield {
        val plans = planSinglePattern(qg, kit, pattern, bestLeafPlansPerAvailableSymbol, qppInnerPlanner, context)
          .map(plan => kit.select(plan, qg))
        val plansWithPrefetchedProperties =
          context.settings.remoteBatchPropertiesStrategy.planPrefetchRemoteBatchPropertiesIfRequired(qg, plans, context)
        // From _all_ plans (even if they are sorted), put the best into the seed
        // with `false`. We don't want to compare just the ones that are unsorted
        // in isolation, because it could be that the best overall plan is sorted.
        val bestWithoutPrefetchedProperties =
          kit.pickBest(
            plans,
            InterestingOrderSelectorHeuristic(context, interestingOrderConfig),
            s"best overall plan for $pattern"
          )
            .map(p =>
              (SolvableItemWithExtraRequirements(Set(pattern), isSorted = false, hasPrefetchedProperties = false), p)
            )
        val bestWithPrefetchedProperties =
          kit.pickBest(
            plansWithPrefetchedProperties,
            InterestingOrderSelectorHeuristic(context, interestingOrderConfig),
            s"best plan with additional properties for $pattern"
          ).map(p =>
            (SolvableItemWithExtraRequirements(Set(pattern), isSorted = false, hasPrefetchedProperties = true), p)
          )
        val result: Iterable[(SolvableItemWithExtraRequirements[NodeConnection], LogicalPlan)] =
          if (interestingOrderConfig.orderToSolve.isEmpty) {
            bestWithoutPrefetchedProperties ++ bestWithPrefetchedProperties
          } else {
            val orderedForAllPlans =
              plans.flatMap(plan =>
                SortPlanner.planIfAsSortedAsPossible(plan, interestingOrderConfig, context)
              )

            // Also add the best sorted plan into the seed with `true`.
            val bestOverallSorted =
              kit.pickBest(orderedForAllPlans, s"best sorted plan for $pattern")
                .map(p =>
                  (SolvableItemWithExtraRequirements(Set(pattern), isSorted = true, hasPrefetchedProperties = false), p)
                )

            bestWithoutPrefetchedProperties ++ bestWithPrefetchedProperties ++ bestOverallSorted
          }

        result
      }).flatten
    // An entirely empty seed while node connections exist means the IDP has no starting point at
    // all and cannot plan the component. Individual relationships may have no direct leaf plan —
    // the IDP reaches them via expansion from adjacent plans — but at least one entry is required.
    if (seed.isEmpty && qg.nodeConnections.nonEmpty) {
      throw InternalException.internalError(
        this.getClass.getSimpleName,
        "Found no access plan for pattern relationships in a connected component. This must not happen."
      )
    }
    seed
  }
}

trait SingleComponentPlannerTrait {

  def planComponent(
    qg: QueryGraph,
    context: LogicalPlanningContext,
    kit: QueryPlannerKit,
    interestingOrderConfig: InterestingOrderConfig
  ): BestPlans

  def planComponent(
    leafPlanCandidates: Set[LogicalPlan],
    qg: QueryGraph,
    context: LogicalPlanningContext,
    kit: QueryPlannerKit,
    interestingOrderConfig: InterestingOrderConfig
  ): BestPlans

  def solverConfig: SingleComponentIDPSolverConfig
}

object SingleComponentPlanner {

  sealed private trait SinglePatternSolutions {
    def getSolutions: Iterable[LogicalPlanWithIntoVsAllHeuristic]
  }

  private case class NonExpandSolutions(solutions: Option[LogicalPlanWithIntoVsAllHeuristic])
      extends SinglePatternSolutions {
    override def getSolutions: Iterable[LogicalPlanWithIntoVsAllHeuristic] = solutions
  }

  private case class ExpandSolutions(
    leftExpand: Option[LogicalPlanWithIntoVsAllHeuristic],
    rightExpand: Option[LogicalPlanWithIntoVsAllHeuristic]
  ) extends SinglePatternSolutions {
    override def getSolutions: Iterable[LogicalPlanWithIntoVsAllHeuristic] = Set(leftExpand, rightExpand).flatten
  }

  /**
   * Plan a single [[NodeConnection]].
   * Return plan candidates on top of all leaf plans where it is possible.
   */
  def planSinglePattern(
    qg: QueryGraph,
    kit: QueryPlannerKit,
    patternToSolve: NodeConnection,
    bestLeafPlansPerAvailableSymbol: Map[Set[LogicalVariable], BestPlans],
    qppInnerPlanner: QPPInnerPlanner,
    context: LogicalPlanningContext
  ): Iterable[LogicalPlan] = {
    val solveds = context.staticComponents.planningAttributes.solveds
    val allLeafPlans = bestLeafPlansPerAvailableSymbol.values.flatMap(_.allResults)
    val leaves =
      allLeafPlans ++ context.settings.remoteBatchPropertiesStrategy.planPrefetchRemoteBatchPropertiesIfRequired(
        qg,
        allLeafPlans,
        context
      )

    val perLeafSolutions: Map[LogicalPlan, SinglePatternSolutions] = leaves.map { leaf =>
      val solvedQg = solveds.get(leaf.id).asSinglePlannerQuery.lastQueryGraph
      val solutions = patternToSolve match {
        case pattern if solvedQg.nodeConnections.contains(pattern) =>
          // if the leaf already solves the pattern, simply return that
          NonExpandSolutions(Some(leaf))
        case _ if solvedQg.nodeConnections.nonEmpty =>
          // Avoid planning an Expand on a plan that already solves another relationship.
          // That is not supposed to happen when we initialize the table, but rather during IDP.
          NonExpandSolutions(None)
        case pattern: PatternRelationship if solvedQg.allCoveredIds.contains(pattern.variable) =>
          NonExpandSolutions(Some(planSingleProjectEndpoints(pattern, leaf, context)))
        case pattern =>
          val (start, end) = pattern.boundaryNodes
          val leftExpand = planSinglePatternSide(qg, pattern, leaf, start, qppInnerPlanner, context)
          val rightExpand = planSinglePatternSide(qg, pattern, leaf, end, qppInnerPlanner, context)
          ExpandSolutions(leftExpand, rightExpand)
      }
      leaf -> solutions
    }.toMap

    val cartesianProductsAndJoins = {
      val (start, end) = patternToSolve.boundaryNodes
      if (start == end) {
        // We are not allowed to plan CP or joins with identical LHS and RHS
        Iterable.empty[LogicalPlanWithIntoVsAllHeuristic]
      } else if (qg.argumentIds.contains(start) || qg.argumentIds.contains(end)) {
        // This kind of join for single relationship patterns is currently only supported for non-argument nodes.
        Iterable.empty[LogicalPlanWithIntoVsAllHeuristic]
      } else {
        val startJoinNodes = Set(start)
        val endJoinNodes = Set(end)

        val maybeStartBestPlans = bestLeafPlansPerAvailableSymbol.get(startJoinNodes ++ qg.argumentIds)
        val maybeEndBestPlans = bestLeafPlansPerAvailableSymbol.get(endJoinNodes ++ qg.argumentIds)

        val cartesianProducts = planSinglePatternCartesianProducts(
          qg,
          kit,
          patternToSolve,
          start,
          maybeStartBestPlans,
          maybeEndBestPlans,
          qppInnerPlanner,
          context
        )

        def getExpandSolutionOnTopOfLeaf(leaf: LogicalPlan): Option[ExpandSolutions] = perLeafSolutions(leaf) match {
          case es: ExpandSolutions => Some(es)
          case _                   => None
        }

        val joins = planSinglePatternJoins(
          qg,
          startJoinNodes,
          endJoinNodes,
          maybeStartBestPlans,
          maybeEndBestPlans,
          getExpandSolutionOnTopOfLeaf,
          context
        ).map(LogicalPlanWithIntoVsAllHeuristic.neutralPlan)

        cartesianProducts ++ joins
      }
    }

    val candidates = perLeafSolutions.values.flatMap(_.getSolutions) ++ cartesianProductsAndJoins
    preFilterCandidatesByIntoVsAllHeuristic(candidates, context)
  }

  private def planSinglePatternCartesianProducts(
    qg: QueryGraph,
    kit: QueryPlannerKit,
    pattern: NodeConnection,
    start: LogicalVariable,
    maybeStartBestPlans: Option[BestPlans],
    maybeEndBestPlans: Option[BestPlans],
    qppInnerPlanner: QPPInnerPlanner,
    context: LogicalPlanningContext
  ): Iterable[LogicalPlanWithIntoVsAllHeuristic] = (maybeStartBestPlans, maybeEndBestPlans) match {
    case (Some(startBestPlan), Some(endBestPlan)) =>
      // CP keeps the LHS order. Therefore we consider both the bestResult and the best sorted result
      // as LHSs of the CP.
      val startLHSEndRHS = startBestPlan.allResults.map { lhsPlan =>
        // CP does not keep the RHS order, so we only consider the best result as the RHS of the CP.
        val rhsPlan = endBestPlan.bestResult
        (lhsPlan, rhsPlan)
      }
      // Analogous
      val endLHSStartRHS = endBestPlan.allResults.map { lhsPlan =>
        val rhsPlan = startBestPlan.bestResult
        (lhsPlan, rhsPlan)
      }

      (startLHSEndRHS ++ endLHSStartRHS).flatMap {
        case (lhsPlan, rhsPlan) =>
          val cp = context.staticComponents.logicalPlanProducer.planCartesianProduct(lhsPlan, rhsPlan, context)
          val cpPlusFilter = kit.select(cp, qg)
          planSinglePatternSide(
            qg,
            pattern,
            cpPlusFilter,
            start,
            qppInnerPlanner,
            context
          )
      }
    case _ => None
  }

  /**
   * If there are hints and the query graph is small, joins have to be constructed as an alternative here, otherwise the hints might not be able to be fulfilled.
   * Creating joins if the query graph is larger will lead to too many joins.
   */
  private def planSinglePatternJoins(
    qg: QueryGraph,
    startJoinNodes: Set[LogicalVariable],
    endJoinNodes: Set[LogicalVariable],
    maybeStartBestPlans: Option[BestPlans],
    maybeEndBestPlans: Option[BestPlans],
    getExpandSolutionOnTopOfLeaf: LogicalPlan => Option[ExpandSolutions],
    context: LogicalPlanningContext
  ): Iterable[LogicalPlan] = (maybeStartBestPlans, maybeEndBestPlans) match {
    case (Some(startBestPlan), Some(endBestPlan)) if qg.hints.nonEmpty && qg.size == 1 =>
      val startJoinHints = qg.joinHints.filter(_.coveredBy(startJoinNodes))
      val endJoinHints = qg.joinHints.filter(_.coveredBy(endJoinNodes))

      // Join keeps the RHS order. Therefore we consider both the bestResult and the best sorted result
      // as RHSs of the join.
      val join1a = endBestPlan.allResults.flatMap { endPlan =>
        // Join does not keep the LHS order, so we only consider the best result as the LHS of the join.
        getExpandSolutionOnTopOfLeaf(startBestPlan.bestResult)
          .flatMap(_.leftExpand)
          .map { leftExpand =>
            context.staticComponents.logicalPlanProducer.planNodeHashJoin(
              endJoinNodes,
              leftExpand.plan,
              endPlan,
              endJoinHints,
              context
            )
          }
      }

      val join1b = startBestPlan.allResults.flatMap(getExpandSolutionOnTopOfLeaf)
        .flatMap(_.leftExpand)
        .map { leftExpand =>
          val endPlan = endBestPlan.bestResult
          context.staticComponents.logicalPlanProducer.planNodeHashJoin(
            endJoinNodes,
            endPlan,
            leftExpand.plan,
            endJoinHints,
            context
          )
        }

      val join2a = endBestPlan.allResults.flatMap(getExpandSolutionOnTopOfLeaf)
        .flatMap(_.rightExpand)
        .map { rightExpand =>
          val startPlan = startBestPlan.bestResult
          context.staticComponents.logicalPlanProducer.planNodeHashJoin(
            startJoinNodes,
            startPlan,
            rightExpand.plan,
            startJoinHints,
            context
          )
        }

      val join2b = startBestPlan.allResults.flatMap { startPlan =>
        getExpandSolutionOnTopOfLeaf(endBestPlan.bestResult)
          .flatMap(_.rightExpand)
          .map { rightExpand =>
            context.staticComponents.logicalPlanProducer.planNodeHashJoin(
              startJoinNodes,
              rightExpand.plan,
              startPlan,
              startJoinHints,
              context
            )
          }
      }
      join1a ++ join1b ++ join2a ++ join2b
    case _ => None
  }
}
