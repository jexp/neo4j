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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.ast.IrHint
import org.neo4j.cypher.internal.ast.UsingScanHint
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LabelOrRelTypeName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.util.collection.immutable.ListSet

trait PlanSelector {

  def apply(
    plan: LogicalPlan,
    queryGraph: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext
  ): LogicalPlan
}

trait CandidateSelector extends ProjectingSelector[LogicalPlan]

object LabelScanLeafPlanner {

  case class HintsAndHintedLabels(
    fulfilledHints: ListSet[UsingScanHint],
    hintedLabels: ListSet[LabelName]
  )

  /**
   * Find all the hints that are fulfilled by a scan on the given variable and prune away the implied labels, unless they are hinted upon.
   *
   * @param hints    the hints present in the query graph
   * @param variable the variable to check for hints
   * @param labels   the labels to prune
   */
  def getHintsAndHintedLabels(
    hints: ListSet[IrHint],
    variable: Variable,
    labels: Iterable[LabelName]
  ): HintsAndHintedLabels = {
    hints.collect {
      case hint @ UsingScanHint(`variable`, LabelOrRelTypeName(name))
        if labels.exists(_.name == name) =>
        (hint, labels.filter(_.name == name))
    }.unzip match {
      case (scanHints, hintedLabels) => HintsAndHintedLabels(scanHints, hintedLabels.flatten)
    }
  }
}
