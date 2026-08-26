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

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.CypherVersionHelpers.versionedSemanticContext
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckContext
import org.neo4j.cypher.internal.ast.semantics.SemanticChecker
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.compiler.ast.convert.plannerQuery.StatementConverters
import org.neo4j.cypher.internal.frontend.helpers.SyntaxExceptionCreator
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.parser.AstParserFactory
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.DummyPosition
import org.neo4j.cypher.internal.util.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.util.Rewritable.RewritableAny
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.helpers.NameDeduplicator.removeGeneratedNamesAndParamsOnTree
import org.neo4j.cypher.internal.util.helpers.fixedPoint

import scala.util.Random
import scala.util.Success
import scala.util.Try

trait PlannerQueryRewriterTest {
  self: CypherPlannerTestSuite =>

  protected def additionalSemanticFeatures: Seq[SemanticFeature] = Seq.empty

  private val allCypherVersions: Set[CypherVersion] = CypherVersion.values().toSet

  private def parse(
    query: String,
    exceptionFactory: CypherExceptionFactory,
    supportedCypherVersions: Set[CypherVersion]
  ): Statement = {
    val defaultCypherVersion = supportedCypherVersions.head
    val defaultStatement = parse(defaultCypherVersion, query, exceptionFactory)

    // Quick and dirty hack to try to make sure we have sufficient coverage of all cypher versions.
    // Feel free to improve ¯\_(ツ)_/¯.
    supportedCypherVersions.foreach { version =>
      if (version != defaultCypherVersion) {
        Try(parse(version, query, exceptionFactory)) match {
          case Success(otherStatement) if otherStatement == defaultStatement => ()
          case notEqual => throw new AssertionError(
              s"""Unexpected result in $version
                 |Default statement: $defaultStatement
                 |$version statement: $notEqual
                 |""".stripMargin
            )
        }
      }
    }

    val unsupportedCypherVersions = allCypherVersions -- supportedCypherVersions
    unsupportedCypherVersions.foreach { version =>
      Try(parse(version, query, exceptionFactory)) match {
        case Success(otherStatement) =>
          throw new AssertionError(
            s"""Unexpected success in $version
               |Default statement: $defaultStatement
               |$version statement: $otherStatement
               |""".stripMargin
          )
        case failure => ()
      }
    }

    defaultStatement
  }

  private def parse(version: CypherVersion, query: String, exceptionFactory: CypherExceptionFactory): Statement = {
    AstParserFactory(version)(query, exceptionFactory, None, Seq()).singleStatement()
  }

  def rewriter(anonymousVariableNameGenerator: AnonymousVariableNameGenerator): Rewriter

  def rewriteAST(
    astOriginal: Statement,
    cypherExceptionFactory: CypherExceptionFactory,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
    semanticState: SemanticState,
    semanticCheckContext: SemanticCheckContext
  ): Statement

  protected def assertRewrite(originalQuery: String, expectedQuery: String): Unit = {

    val expectedGen = new AnonymousVariableNameGenerator()
    val actualGen = new AnonymousVariableNameGenerator()
    val expected = removeGeneratedNamesAndParamsOnTree(
      getTheWholePlannerQueryFrom(expectedQuery.stripMargin, expectedGen, allCypherVersions)
    )
    val original = getTheWholePlannerQueryFrom(originalQuery.stripMargin, actualGen, allCypherVersions)

    val result = removeGeneratedNamesAndParamsOnTree(
      original.endoRewrite(fixedPoint(CancellationChecker.neverCancelled())(rewriter(actualGen)))
    )
    assert(
      result === expected,
      s"""$originalQuery
         |Was not rewritten correctly:
         |  Expected:
         |$expected
         |  But got:
         |$result""".stripMargin
    )
  }

  protected def assertRewriteMultiple(originalQuery: String, expectedQueries: String*): Unit = {
    val expected =
      expectedQueries.map { query =>
        val expectedGen = new AnonymousVariableNameGenerator()
        removeGeneratedNamesAndParamsOnTree(
          getTheWholePlannerQueryFrom(query.stripMargin, expectedGen, allCypherVersions)
        )
      }
    val actualGen = new AnonymousVariableNameGenerator()
    val original = getTheWholePlannerQueryFrom(originalQuery.stripMargin, actualGen, allCypherVersions)

    val result = removeGeneratedNamesAndParamsOnTree(
      original.endoRewrite(fixedPoint(CancellationChecker.neverCancelled())(rewriter(actualGen)))
    )
    assert(
      expected.exists(_ === result),
      s"""$originalQuery
         |Was not rewritten correctly:
         |  Expected any of:
         |${expected.mkString("\n")}
         |  But got:
         |$result""".stripMargin
    )
  }

  protected def assertIsNotRewritten(query: String): Unit = {
    assertIsNotRewritten(query, allCypherVersions)
  }

  protected def assertIsNotRewritten(query: String, supportedCypherVersions: Set[CypherVersion]): Unit = {
    val actualGen = new AnonymousVariableNameGenerator()
    val plannerQuery = getTheWholePlannerQueryFrom(query.stripMargin, actualGen, supportedCypherVersions)
    val result = plannerQuery.endoRewrite(fixedPoint(CancellationChecker.neverCancelled())(rewriter(actualGen)))
    assert(result === plannerQuery, "\nShould not have been rewritten\n" + query)
  }

  private def getTheWholePlannerQueryFrom(
    query: String,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
    supportedCypherVersions: Set[CypherVersion]
  ): PlannerQuery = {

    val randomSupportedVersion = Random.shuffle(supportedCypherVersions).head
    val semanticCheckContext = versionedSemanticContext(randomSupportedVersion)
    val semanticState = SemanticState.clean.withFeatures(additionalSemanticFeatures)
    val exceptionFactory = Neo4jCypherExceptionFactory(query, Some(DummyPosition(0)))

    val astOriginal =
      parse(query.replace("\r\n", "\n"), exceptionFactory, supportedCypherVersions)

    val ast =
      rewriteAST(astOriginal, exceptionFactory, anonymousVariableNameGenerator, semanticState, semanticCheckContext)
    val onError = SyntaxExceptionCreator.throwOnError(exceptionFactory)
    val result =
      SemanticChecker.check(ast, semanticState, semanticCheckContext)
    onError(result.errors)
    val table = SemanticTable(
      types = result.state.typeTable,
      recordedScopes = result.state.recordedScopes.view.mapValues(_.scope).toMap
    )
    StatementConverters.withDefaultConfig.convertToPlannerQuery(
      ast.asInstanceOf[Query],
      table,
      anonymousVariableNameGenerator,
      CancellationChecker.NeverCancelled
    )
  }
}
