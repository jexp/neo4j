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
package org.neo4j.cypher.internal.frontend.scoping.checker

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.ScopeQueries
import org.neo4j.cypher.internal.ast.semantics.scoping.NoResult
import org.neo4j.cypher.internal.ast.semantics.scoping.TableResult
import org.neo4j.cypher.internal.frontend.helpers.ErrorCollectingContext
import org.neo4j.cypher.internal.frontend.helpers.NoPlannerName
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.Parse
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.ScopeSurveyor
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import java.io.File
import java.lang.reflect.Modifier

// Wrong TestQuery.returnColumns silently produces malformed wrapping queries in
// SurroundGivenQueriesWithLocalCallablesDefinition, surfacing as flakiness elsewhere rather than here.
class TestQueryReturnColumnsTest extends CypherFunSuite {

  private val packageName = "org.neo4j.cypher.internal.frontend.scoping.checker"

  // Constructs every GQL_* suite ourselves, since Surefire randomizes class order and
  // VariableCheckingWithLocalCallablesTestSuite's registry is only populated on construction.
  private def instantiateAllTestQueryProducers(): Unit = {
    val packagePath = packageName.replace('.', '/')
    val dir = new File(getClass.getClassLoader.getResource(packagePath).toURI)
    val producerClass = classOf[VariableCheckingWithLocalCallablesTestSuite]
    dir.listFiles((_, name) => name.endsWith(".class") && !name.contains("$")).foreach { file =>
      val cls = Class.forName(s"$packageName.${file.getName.stripSuffix(".class")}")
      if (producerClass.isAssignableFrom(cls) && !Modifier.isAbstract(cls.getModifiers)) {
        cls.getDeclaredConstructor().newInstance()
      }
    }
  }

  private def actualReturnColumns(query: String, version: CypherVersion): Option[Seq[String]] = {
    try {
      val context = new ErrorCollectingContext(version, semanticFeatures = Seq(ScopeQueries))
      val initialState = InitialState(query, NoPlannerName, new AnonymousVariableNameGenerator)
      val state = (Parse andThen ScopeSurveyor).transform(initialState, context)
      state.maybeScopeState.map(_.workingScope.result).collect {
        case TableResult(columns) => columns.map(_.name)
        case NoResult             => Seq.empty
      }
    } catch {
      case _: Exception => None
    }
  }

  private def unescape(name: String): String =
    if (name.length >= 2 && name.startsWith("`") && name.endsWith("`")) name.substring(1, name.length - 1)
    else name

  test("all TestQuery.returnColumns match what ScopeSurveyor actually resolves") {
    instantiateAllTestQueryProducers()
    val allTestQueries = VariableCheckingWithLocalCallablesTestSuite.getAllTestCases

    val mismatches = for {
      tq <- allTestQueries
      if !tq.cypher.contains("SHOW TRANSACTIONS YIELD")
      declared = tq.returnColumns.map(unescape)
      version <- CypherVersion.values().toSeq
      actual <- actualReturnColumns(tq.cypher, version)
      if actual.sorted != declared.sorted
    } yield s"""Version: $version
               |Query:
               |${tq.cypher}
               |
               |Declared returnColumns: ${tq.returnColumns}
               |Actual returnColumns:   $actual
               |""".stripMargin

    withClue(s"${mismatches.size} TestQuery.returnColumns mismatch(es):\n\n${mismatches.mkString("\n")}") {
      mismatches shouldBe empty
    }
  }
}
