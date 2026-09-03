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
package org.neo4j.cypher.cucumber.synthesise.generator

import org.neo4j.cypher.cucumber.glue.regular.CompositeExecutorPool
import org.neo4j.cypher.cucumber.steps.Result
import org.neo4j.cypher.cucumber.synthesise.CucumberSalad
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.AssertApproxResults
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.AssertGqlError
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.AssertGqlWarning
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.AssertResults
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.Execute
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.ExecuteControl
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.ExecuteControlInOpenTx
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.ExecuteInOpenTx
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.HavingExecuted
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.HavingExecutedInOpenTx
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.QueryExecution
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.RecordedScenario

import java.lang.Boolean.getBoolean
import java.nio.file.Paths
import java.util.concurrent.atomic.AtomicLong

import scala.collection.View

/**
 * Shared scaffolding for the composite generators (topology in [[CompositeExecutorPool]]). Setup steps are always
 * wrapped stitched so their writes persist against the composite session db; only test/control steps get the
 * subclass's [[testWrap]]. Every subclass is gated by the composite config's `@fails:composite` / `@ignore:composite`
 * and by its own `@fails:<name>` tag.
 */
abstract class CompositeGenerator(val args: CucumberSalad.Ingredients) extends ScenarioGenerator {

  private val counter = new AtomicLong(0)

  // Opt-in via system property: also emit the `@fails:composite`-muted scenarios, to triage which still fail.
  private val includeMuted: Boolean = getBoolean(CompositeGenerator.IncludeMutedProperty)

  /** The targeted constituent alias; [[CompositeLocal]] overrides it with a local one. */
  protected def constituent: String = CompositeExecutorPool.Constituent

  final protected def usePrefix: String = CompositeUse.usePrefixFor(constituent)

  final protected def stitchedWrap(cypher: String): String =
    CompositeUse.wrapStitched(args.parser, cypher, usePrefix)

  protected def testWrap(cypher: String): String

  protected def namePrefix: String
  protected def wrapDescription: String

  /** Step/query predicates common to every composite generator; subclasses append their own via `override`. */
  protected def compositeFilter: Filter = compatibilityBase
    .steps[AssertGqlError](_.isEmpty)
    .steps[AssertApproxResults](_.isEmpty)
    .steps[AssertResults](_.forall(r => !r.assertion.isInstanceOf[Result.ParallelOverride]))
    .queries[QueryExecution](_.forall(q => Filter.isNotCommand(q) && CompositeUse.isSingleGraphWrappable(q)))
    .testQueries(_.nonEmpty)

  override def filter: Filter = compositeFilter

  // With includeMuted we still exclude ignores/generator-scoped/@conf tags but keep `@fails:composite` scenarios.
  private def compatibilityBase: Filter =
    if (includeMuted)
      Filter(args.parser, Seq.empty)
        .scenario(Filter.isNotIgnored(args.targetConf))
        .scenario(Filter.excludeTags(excludedTags: _*))
        .scenario(Filter.isNotConfScoped)
    else super.filter

  override def generateScenarios(filteredScenarios: View[RecordedScenario]): IterableOnce[GeneratedScenario] =
    filteredScenarios.map(generateScenario)

  private def generateScenario(scenario: RecordedScenario): GeneratedScenario = {
    val steps = scenario.steps.collect {
      case HavingExecuted(c)         => HavingExecuted(stitchedWrap(c))
      case HavingExecutedInOpenTx(c) => HavingExecutedInOpenTx(stitchedWrap(c))
      case Execute(c)                => Execute(testWrap(c))
      case ExecuteInOpenTx(c)        => ExecuteInOpenTx(testWrap(c))
      case ExecuteControl(c)         => ExecuteControl(testWrap(c))
      case ExecuteControlInOpenTx(c) => ExecuteControlInOpenTx(testWrap(c))
      // Notification assertions are dropped: the wrap itself changes which notifications the query raises.
      case step if !step.isInstanceOf[AssertGqlWarning] => step
    }
    GeneratedScenario(
      name = s"$namePrefix ${counter.incrementAndGet()}: ${scenario.name}",
      steps = steps,
      featurePath = Paths.get(scenario.uri.getSchemeSpecificPart),
      comment = s"Generated by $wrapDescription, based on ${scenario.source}",
      tags = if (includeMuted) scenario.tags - CompositeGenerator.MuteTag else scenario.tags
    )
  }
}

object CompositeGenerator {
  val MuteTag = "@fails:composite"
  val IncludeMutedProperty = "cypher.synthesise.composite.include_muted"
}
