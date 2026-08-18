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

import io.cucumber.datatable.DataTable
import org.neo4j.cypher.cucumber.steps.Result
import org.neo4j.cypher.cucumber.steps.Result.Single
import org.neo4j.cypher.cucumber.synthesise.CucumberSalad
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.AssertResults
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.Execute
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.RecordedScenario
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.RecordedStep
import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.Limit
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Skip
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.expressions.Add
import org.neo4j.cypher.internal.expressions.IntegerLiteral
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Ref
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.topDown

import java.nio.file.Paths
import java.util.concurrent.atomic.AtomicLong

import scala.collection.View
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.util.Random

class Paginate(val args: CucumberSalad.Ingredients) extends ScenarioGenerator with ScenarioRenderer {
  override val name: String = "paginate"

  private val prettifier = Prettifier(ExpressionStringifier(alwaysBacktick = true))
  private val counter = new AtomicLong(0)

  def pos: InputPosition = InputPosition.NONE

  override def filter: Filter = super.filter
    .steps[AssertResults](_.exists(_.rowCount > 1)) // Only scenarios with multi row results
    .testsReadQueries

  def generateScenarios(filteredScenarios: View[RecordedScenario]): IterableOnce[GeneratedScenario] = {
    filteredScenarios.flatMap { scenario =>
      val stepReplacements = scenario.steps.sliding(2).flatMap {
        case Seq(e: Execute, a @ AssertResults(_, Single(Result.Assertion(Result.Order.Ordered, _, _))))
          if a.rowCount > 1 =>
          transformAssertion(e, a) match {
            case Some(newSteps) => Seq(Ref(e) -> newSteps, Ref(a) -> Seq.empty[RecordedStep])
            case None           => Seq.empty
          }
        case _ => Seq.empty
      }
      Option.when(stepReplacements.nonEmpty)(generateScenario(scenario, stepReplacements.toMap))
    }
  }

  private def transformAssertion(e: Execute, a: AssertResults): Option[Seq[RecordedStep]] = {
    args.parser.parse(e.cypher).ast match {
      case SingleQueryWithReturn(statement, ret, existingLimit) =>
        val columnNames = a.expected.row(0)
        val skipLimitCombinations = for {
          skip <- Range(0, a.rowCount)
          limit <- Range.inclusive(1, existingLimit.map(_ - skip).getOrElse(a.rowCount))
        } yield skip -> limit
        val skipLimit = new Random(args.rand.nextInt()).shuffle(skipLimitCombinations).take(4)
        val newSteps = skipLimit.flatMap { case (skip, limit) =>
          val skipLiteral = SignedDecimalIntegerLiteral(skip.toString)(pos.zeroLength)
          val newSkip = ret.skip.map(_.expression).map(Add(_, skipLiteral)(pos)).getOrElse(skipLiteral)
          val newItems = ret.returnItems.items.zipWithIndex.map { case (item, index) =>
            val header = columnNames.get(index)
            val name = item match {
              case ari: AliasedReturnItem =>
                logicalVariableForHeader(header, Some(ari.variable))
              case _ =>
                logicalVariableForHeader(header, None)
            }
            AliasedReturnItem(item.expression, name)(pos, AliasedReturnItem.wasAutoAliasedDefault)
          }
          val newReturn = ret.copy(
            returnItems = ret.returnItems.copy(items = newItems)(pos),
            skip = Some(Skip(newSkip)(pos)),
            limit = Some(Limit(SignedDecimalIntegerLiteral(limit.toString)(pos.zeroLength))(pos))
          )(pos)
          val newStatement = statement.endoRewrite(topDown(Rewriter.lift {
            case oldReturn if oldReturn eq ret => newReturn
          }))
          val expectedRows = a.expected.cells().stream().skip(skip + 1).limit(limit).toList.asScala
          val expectedTable = expectedRows.prepended(columnNames).asJava
          Seq(
            Execute(prettifier.asString(newStatement)),
            a.copy(expected = DataTable.create(expectedTable))
          )
        }
        Some(newSteps).filter(_.nonEmpty)
      case _ => None
    }
  }

  /** Keep parser isolation (e.g. `null`) when the result header matches the original projection alias. */
  private def logicalVariableForHeader(header: String, original: Option[LogicalVariable]): LogicalVariable =
    original match {
      case Some(v: Variable) if v.name == header => Variable(v.name)(pos, v.isIsolated)
      case _                                     => Variable(header)(pos, Variable.isIsolatedDefault)
    }

  private def generateScenario(
    scenario: RecordedScenario,
    replacements: Map[Ref[RecordedStep], Seq[RecordedStep]]
  ): GeneratedScenario = {
    val newSteps = scenario.steps.flatMap(step => replacements.getOrElse(Ref(step), Seq(step)))
    val name = s"Generated scenario ${counter.incrementAndGet()}"
    val featurePath = Paths.get(scenario.uri.getSchemeSpecificPart)
    val comment = "Generated by adding pagination (SKIP + LIMIT), based on " + scenario.source
    GeneratedScenario(name, newSteps, featurePath, comment, scenario.tags)
  }
}

object SingleQueryWithReturn {

  def unapply(statement: Statement): Option[(SingleQuery, Return, Option[Int])] = statement match {
    case q @ SingleQuery(clauses) => clauses.lastOption.collect {
        case ret: Return if ret.limit.isEmpty => (q, ret, None)
        case ret @ Return(_, _, _, _, _, Some(Limit(limit: IntegerLiteral)), _, _, _)
          if limit.value > 0 && limit.value < Int.MaxValue => (q, ret, Some(limit.value.toInt))
      }
    case _ => None
  }
}
