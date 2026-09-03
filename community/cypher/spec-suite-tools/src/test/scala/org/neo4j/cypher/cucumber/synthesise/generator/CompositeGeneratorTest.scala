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
import org.neo4j.cypher.cucumber.glue.regular.CompositeExecutorPool
import org.neo4j.cypher.cucumber.glue.regular.TestConf
import org.neo4j.cypher.cucumber.steps.Result
import org.neo4j.cypher.cucumber.synthesise.CucumberSalad
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.AssertApproxResults
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.AssertGqlError
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.AssertGqlWarning
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.AssertResults
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.CommitTransaction
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.Execute
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.ExecuteControl
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.HavingExecuted
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.OpenTransaction
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.RecordedScenario
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.RecordedStep
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.SideEffects
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.test.RandomSupport

import java.net.URI
import java.nio.file.Paths

import scala.collection.View

class CompositeGeneratorTest extends CypherFunSuite {

  private def ingredients(): CucumberSalad.Ingredients =
    CucumberSalad.Ingredients(Seq.empty, new RandomSupport(), Paths.get("unused"), TestConf.Composite.Cypher25.conf)

  private def stitched(): CompositeStitched = new CompositeStitched(ingredients())
  private def local(): CompositeLocal = new CompositeLocal(ingredients())

  private def scenario(steps: Seq[RecordedStep], tags: Set[String] = Set.empty): RecordedScenario =
    RecordedScenario(new URI("features/Example.feature"), 1, "example", steps, tags)

  private val row = java.util.List.of(java.util.List.of("n"), java.util.List.of("1"))
  private def results(assertion: Result.Assertions) = AssertResults(DataTable.create(row), assertion)

  test("compositeFilter keeps a plain read scenario with a test query") {
    val s = scenario(Seq(
      HavingExecuted("CREATE ({p: 1})"),
      Execute("MATCH (n) RETURN n.p AS x"),
      results(Result.Single(Result.InAnyOrder))
    ))
    stitched().filter.build(s) shouldBe true
  }

  test("compositeFilter drops error-expecting scenarios") {
    val s = scenario(Seq(Execute("MATCH (n) RETURN n.p AS x"), AssertGqlError(null)))
    stitched().filter.build(s) shouldBe false
  }

  test("compositeFilter drops approximate-result scenarios") {
    val s = scenario(Seq(Execute("MATCH (n) RETURN n.p AS x"), AssertApproxResults(DataTable.create(row), 1)))
    stitched().filter.build(s) shouldBe false
  }

  test("compositeFilter drops scenarios whose result assertion is parallel-runtime dependent") {
    val s = scenario(Seq(
      Execute("MATCH (n) RETURN n.p AS x"),
      results(Result.ParallelOverride(Result.InOrder, Result.InAnyOrder))
    ))
    stitched().filter.build(s) shouldBe false
  }

  test("compositeFilter drops command queries, in setup as well as in the test") {
    val test = scenario(Seq(Execute("CREATE INDEX FOR (n:L) ON (n.p)")))
    val setup = scenario(Seq(HavingExecuted("CREATE INDEX FOR (n:L) ON (n.p)"), Execute("MATCH (n) RETURN n AS n")))
    val g = stitched()
    g.filter.build(test) shouldBe false
    g.filter.build(setup) shouldBe false
  }

  test("compositeFilter drops queries that already select a graph") {
    val s = scenario(Seq(Execute("USE neo4j MATCH (n) RETURN n AS n")))
    stitched().filter.build(s) shouldBe false
  }

  test("compositeFilter drops scenarios with no test query") {
    stitched().filter.build(scenario(Seq(HavingExecuted("CREATE ()")))) shouldBe false
  }

  test("compositeFilter drops scenarios muted for the composite config") {
    val muted = scenario(Seq(Execute("MATCH (n) RETURN n AS n")), tags = Set(CompositeGenerator.MuteTag))
    val ignored = scenario(Seq(Execute("MATCH (n) RETURN n AS n")), tags = Set("@ignore:composite"))
    val g = stitched()
    g.filter.build(muted) shouldBe false
    g.filter.build(ignored) shouldBe false
  }

  test("setup steps are wrapped stitched even when the test wrap differs") {
    val s = scenario(Seq(HavingExecuted("CREATE ({p: 1})"), Execute("MATCH (n) RETURN n.p AS x")))
    val generated = new CompositeSeparate(ingredients()).generateScenarios(View(s)).iterator.toSeq
    generated.head.steps shouldBe Seq(
      HavingExecuted(s"USE ${CompositeExecutorPool.Constituent}\nCREATE ({p: 1})"),
      Execute(
        s"CALL () {\nUSE ${CompositeExecutorPool.Constituent}\nMATCH (n) RETURN n.p AS x\n}\nRETURN `x`"
      )
    )
  }

  test("control queries are wrapped like test queries, and other steps are kept verbatim") {
    val s = scenario(Seq(
      Execute("CREATE ({p: 1})"),
      ExecuteControl("MATCH (n) RETURN n.p AS x"),
      results(Result.Single(Result.InAnyOrder)),
      SideEffects(DataTable.emptyDataTable())
    ))
    val generated = stitched().generateScenarios(View(s)).iterator.toSeq
    generated.head.steps shouldBe Seq(
      Execute(s"USE ${CompositeExecutorPool.Constituent}\nCREATE ({p: 1})"),
      ExecuteControl(s"USE ${CompositeExecutorPool.Constituent}\nMATCH (n) RETURN n.p AS x"),
      results(Result.Single(Result.InAnyOrder)),
      SideEffects(DataTable.emptyDataTable())
    )
  }

  test("notification assertions are dropped, since the wrap changes which notifications are raised") {
    val s = scenario(Seq(Execute("MATCH (n) RETURN n.p AS x"), AssertGqlWarning(null)))
    val generated = stitched().generateScenarios(View(s)).iterator.toSeq
    generated.head.steps shouldBe Seq(Execute(s"USE ${CompositeExecutorPool.Constituent}\nMATCH (n) RETURN n.p AS x"))
  }

  test("generated scenarios are numbered, keep the source tags and record provenance") {
    val s = scenario(Seq(Execute("MATCH (n) RETURN n AS n")), tags = Set("@fails:parallel-runtime"))
    val generated = stitched().generateScenarios(View(s, s)).iterator.toSeq
    generated.map(_.name) shouldBe Seq("Composite 1: example", "Composite 2: example")
    generated.head.tags shouldBe Set("@fails:parallel-runtime")
    generated.head.comment should include("composite-wrapping")
    generated.head.comment should include("based on")
  }

  test("includeMuted keeps composite-muted scenarios and strips the mute tag from the generated ones") {
    val muted = scenario(Seq(Execute("MATCH (n) RETURN n AS n")), tags = Set(CompositeGenerator.MuteTag, "@other"))
    val ignored = scenario(Seq(Execute("MATCH (n) RETURN n AS n")), tags = Set("@ignore:composite"))
    val generatorMuted = scenario(Seq(Execute("MATCH (n) RETURN n AS n")), tags = Set("@fails:composite-stitched"))
    val confScoped = scenario(Seq(Execute("MATCH (n) RETURN n AS n")), tags = Set("@conf:something"))
    withIncludeMuted {
      val g = stitched()
      g.filter.build(muted) shouldBe true
      g.filter.build(ignored) shouldBe false
      g.filter.build(generatorMuted) shouldBe false
      g.filter.build(confScoped) shouldBe false
      g.generateScenarios(View(muted)).iterator.toSeq.head.tags shouldBe Set("@other")
    }
  }

  test("composite-local targets the local constituent") {
    val s = scenario(Seq(HavingExecuted("CREATE ()"), Execute("MATCH (n) RETURN n AS n")))
    val generated = local().generateScenarios(View(s)).iterator.toSeq
    generated.head.name shouldBe "Composite-local 1: example"
    generated.head.steps shouldBe Seq(
      HavingExecuted(s"USE ${CompositeExecutorPool.LocalConstituent}\nCREATE ()"),
      Execute(s"USE ${CompositeExecutorPool.LocalConstituent}\nMATCH (n) RETURN n AS n")
    )
  }

  test("composite-local drops open-transaction scenarios, which the stitched generator keeps") {
    val openTx = scenario(Seq(OpenTransaction, Execute("MATCH (n) RETURN n AS n"), CommitTransaction))
    local().filter.build(openTx) shouldBe false
    stitched().filter.build(openTx) shouldBe true
  }

  private def withIncludeMuted(assertions: => Unit): Unit = {
    System.setProperty(CompositeGenerator.IncludeMutedProperty, "true")
    try assertions
    finally System.clearProperty(CompositeGenerator.IncludeMutedProperty)
  }
}
