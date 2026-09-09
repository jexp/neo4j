/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.ast.semantics.functions

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.DummyExpression
import org.neo4j.cypher.internal.ast.SemanticCheckInTest.SemanticCheckWithDefaultContext
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckResult
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticExpressionCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticFunSuite
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.Expression.SemanticContext
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.util.DummyPosition
import org.neo4j.cypher.internal.util.FunctionName
import org.neo4j.cypher.internal.util.symbols.TypeSpec
import org.neo4j.gqlstatus.ErrorGqlStatusObject

abstract class FunctionTestBase(funcName: String) extends SemanticFunSuite {

  protected val context: SemanticContext = SemanticContext.Simple

  protected def testValidTypes(argumentTypes: TypeSpec*)(expected: TypeSpec): Unit = {
    testValidTypesInVersion(argumentTypes: _*)(_ => expected)
  }

  protected def testValidTypesInVersion(argumentTypes: TypeSpec*)(expected: CypherVersion => TypeSpec): Unit = {
    assertInVersions(argumentTypes) { case (language, result, invocation) =>
      result.errors shouldBe empty
      types(invocation)(result.state) should equal(expected(language))
    }
  }

  protected def testInvalidApplication(argumentTypes: TypeSpec*)(message: String): Unit = {
    testInvalidApplicationInVersion(argumentTypes: _*)(_ => message)
  }

  protected def testInvalidApplicationInVersion(argumentTypes: TypeSpec*)(message: CypherVersion => String): Unit = {
    assertInVersions(argumentTypes) { case (language, result, _) =>
      result.errors should not be empty
      result.errors.head.msg should equal(message(language))
    }
  }

  protected def testInvalidApplicationInSpecificVersion(
    version: CypherVersion,
    argumentTypes: TypeSpec*
  )(message: String): Unit = {
    withClue(s"Cypher version: $version") {
      val (result, _) = evaluateWithTypes(version, argumentTypes.toIndexedSeq)
      result.errors should not be empty
      result.errors.head.msg should equal(message)
    }
  }

  protected def testInvalidApplicationWithGql(
    argumentTypes: TypeSpec*
  )(
    message: String
  )(
    gql: ErrorGqlStatusObject
  ): Unit = {
    assertInVersions(argumentTypes) { case (_, result, _) =>
      result.errors should not be empty
      result.errors.head.msg should equal(message)
      result.errors.head.asInstanceOf[SemanticError].gqlStatusObject should equal(gql)
    }
  }

  protected def evaluateWithTypes(
    language: CypherVersion,
    argumentTypes: IndexedSeq[TypeSpec]
  ): (SemanticCheckResult, FunctionInvocation) = {
    val arguments = argumentTypes.map(DummyExpression(_))

    val invocation = FunctionInvocation(
      FunctionName(funcName)(DummyPosition(6)),
      distinct = false,
      arguments
    )(DummyPosition(5))

    val state = SemanticExpressionCheck.check(context, arguments).run(SemanticState.clean, language).state
    (SemanticExpressionCheck.check(context, invocation).run(state, language), invocation)
  }

  private def assertInVersions(types: Seq[TypeSpec])(f: (
    CypherVersion,
    SemanticCheckResult,
    FunctionInvocation
  ) => Unit): Unit = {
    CypherVersion.values().foreach { language =>
      withClue(s"Cypher version: $language") {
        val (result, invocation) = evaluateWithTypes(language, types.toIndexedSeq)
        f(language, result, invocation)
      }
    }
  }
}

trait AggregationFunctionTestBase {
  self: FunctionTestBase =>

  override protected val context: SemanticContext = SemanticContext.Results
}
