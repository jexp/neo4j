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
package org.neo4j.cypher.internal.ast.factory.ddl

import org.neo4j.cypher.internal.ast.CreateDatabase
import org.neo4j.cypher.internal.ast.IfExistsDoNothing
import org.neo4j.cypher.internal.ast.IfExistsInvalidSyntax
import org.neo4j.cypher.internal.ast.IfExistsReplace
import org.neo4j.cypher.internal.ast.IfExistsThrowError
import org.neo4j.cypher.internal.ast.IndefiniteWait
import org.neo4j.cypher.internal.ast.NamespacedName
import org.neo4j.cypher.internal.ast.NoOptions
import org.neo4j.cypher.internal.ast.NoWait
import org.neo4j.cypher.internal.ast.OptionsMap
import org.neo4j.cypher.internal.ast.OptionsParam
import org.neo4j.cypher.internal.ast.ShardDefinition
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.TimeoutAfter
import org.neo4j.cypher.internal.ast.Topology
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.test_helpers.GqlExceptionMatchers.gqlStatus
import org.neo4j.gqlstatus.GqlStatusInfoCodes

class CreateDatabaseAdministrationCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {

  test("CREATE DATABASE foo SET PROPERTY SHARDS { COUNT 2 }") {
    parsesIn[Statements] {
      case Cypher5 => _.withSyntaxErrorContaining(
          "Invalid input 'SET': expected a database name, 'DEFAULT LANGUAGE CYPHER', 'IF NOT EXISTS', 'NOWAIT', 'OPTIONS', 'TOPOLOGY', 'WAIT' or <EOF> (line 1, column 21 (offset: 20))"
        )
      case _ => _.toAstPositioned(CreateDatabase(
          literalFoo,
          IfExistsThrowError,
          NoOptions,
          NoWait()(pos),
          None,
          None,
          Some(ShardDefinition(2, None, None))
        )(pos))
    }
  }

  test("CREATE DATABASE foo SET PROPERTY SHARDS { COUNT 2 TOPOLOGY 3 REPLICAS }") {
    parsesIn[Statements] {
      case Cypher5 => _.withSyntaxErrorContaining(
          "Invalid input 'SET': expected a database name, 'DEFAULT LANGUAGE CYPHER', 'IF NOT EXISTS', 'NOWAIT', 'OPTIONS', 'TOPOLOGY', 'WAIT' or <EOF> (line 1, column 21 (offset: 20))"
        )
      case _ => _.toAstPositioned(CreateDatabase(
          literalFoo,
          IfExistsThrowError,
          NoOptions,
          NoWait()(pos),
          None,
          None,
          Some(ShardDefinition(2, None, Some(Left(3))))
        )(pos))
    }
  }

  test(
    "CREATE DATABASE foo SET GRAPH SHARD { TOPOLOGY 3 PRIMARY 1 SECONDARY } SET PROPERTY SHARDS { COUNT 2 TOPOLOGY 3 REPLICAS }"
  ) {
    parsesIn[Statements] {
      case Cypher5 => _.withSyntaxErrorContaining(
          "Invalid input 'SET': expected a database name, 'DEFAULT LANGUAGE CYPHER', 'IF NOT EXISTS', 'NOWAIT', 'OPTIONS', 'TOPOLOGY', 'WAIT' or <EOF> (line 1, column 21 (offset: 20))"
        )
      case _ => _.toAstPositioned(CreateDatabase(
          literalFoo,
          IfExistsThrowError,
          NoOptions,
          NoWait()(pos),
          None,
          None,
          Some(ShardDefinition(2, Some(Topology(Some(Left(3)), Some(Left(1)))), Some(Left(3))))
        )(pos))
    }
  }

  test(
    "CREATE DATABASE $foo SET GRAPH SHARD { TOPOLOGY 3 PRIMARY 1 SECONDARY } SET PROPERTY SHARDS { COUNT 2 TOPOLOGY 3 REPLICAS }"
  ) {
    parsesIn[Statements] {
      case Cypher5 => _.withSyntaxErrorContaining(
          "Invalid input 'SET': expected 'DEFAULT LANGUAGE CYPHER', 'IF NOT EXISTS', 'NOWAIT', 'OPTIONS', 'TOPOLOGY', 'WAIT' or <EOF> (line 1, column 22 (offset: 21))"
        )
      case _ => _.toAstPositioned(CreateDatabase(
          stringParamName("foo"),
          IfExistsThrowError,
          NoOptions,
          NoWait()(pos),
          None,
          None,
          Some(ShardDefinition(2, Some(Topology(Some(Left(3)), Some(Left(1)))), Some(Left(3))))
        )(pos))
    }
  }

  test("CREATE DATABASE foo SET GRAPH SHARD {} SET PROPERTY SHARDS { COUNT 2 }") {
    parsesIn[Statements] {
      case Cypher5 => _.withSyntaxErrorContaining(
          "Invalid input 'SET': expected a database name, 'DEFAULT LANGUAGE CYPHER', 'IF NOT EXISTS', 'NOWAIT', 'OPTIONS', 'TOPOLOGY', 'WAIT' or <EOF> (line 1, column 21 (offset: 20))"
        )
      case _ => _.toAstPositioned(CreateDatabase(
          literalFoo,
          IfExistsThrowError,
          NoOptions,
          NoWait()(pos),
          None,
          None,
          Some(ShardDefinition(2, None, None))
        )(pos))
    }
  }

  test("CREATE DATABASE foo") {
    parsesTo[Statements](CreateDatabase(
      literalFoo,
      IfExistsThrowError,
      NoOptions,
      NoWait()(pos),
      None,
      None,
      None
    )(pos))
  }

  test("USE system CREATE DATABASE foo") {
    // can parse USE clause, but is not included in AST
    def expected(resolveStrictly: Boolean) = {
      CreateDatabase(literalFoo, IfExistsThrowError, NoOptions, NoWait()(pos), None, None, None)(pos)
        .withGraph(Some(use(List("system"), resolveStrictly)))
    }

    parsesIn[Statement] {
      case Cypher5 => _.toAst(expected(resolveStrictly = false))
      case _       => _.toAst(expected(resolveStrictly = true))
    }
  }

  test("CREATE DATABASE $foo") {
    parsesTo[Statements](CreateDatabase(
      stringParamName("foo"),
      IfExistsThrowError,
      NoOptions,
      NoWait()(pos),
      None,
      None,
      None
    )(pos))
  }

  test("CREATE DATABASE $wait") {
    parsesTo[Statements](CreateDatabase(
      stringParamName("wait"),
      IfExistsThrowError,
      NoOptions,
      NoWait()(pos),
      None,
      None,
      None
    )(pos))
  }

  test("CREATE DATABASE `nowait.sec`") {
    parsesTo[Statements](CreateDatabase(
      literal("nowait.sec"),
      IfExistsThrowError,
      NoOptions,
      NoWait()(pos),
      None,
      None,
      None
    )(pos))
  }

  test("CREATE DATABASE second WAIT") {
    parsesTo[Statements](CreateDatabase(
      literal("second"),
      IfExistsThrowError,
      NoOptions,
      IndefiniteWait()(defaultPos),
      None,
      None,
      None
    )(pos))
  }

  test("CREATE DATABASE seconds WAIT 12") {
    parsesTo[Statements](CreateDatabase(
      literal("seconds"),
      IfExistsThrowError,
      NoOptions,
      TimeoutAfter("12")(defaultPos),
      None,
      None,
      None
    )(pos))
  }

  test("CREATE DATABASE dump WAIT 12 SEC") {
    parsesTo[Statements](CreateDatabase(
      literal("dump"),
      IfExistsThrowError,
      NoOptions,
      TimeoutAfter("12")(defaultPos),
      None,
      None,
      None
    )(pos))
  }

  test("CREATE DATABASE destroy WAIT 12 SECOND") {
    parsesTo[Statements](CreateDatabase(
      literal("destroy"),
      IfExistsThrowError,
      NoOptions,
      TimeoutAfter("12")(defaultPos),
      None,
      None,
      None
    )(pos))
  }

  test("CREATE DATABASE data WAIT 12 SECONDS") {
    parsesTo[Statements](CreateDatabase(
      literal("data"),
      IfExistsThrowError,
      NoOptions,
      TimeoutAfter("12")(defaultPos),
      None,
      None,
      None
    )(pos))
  }

  test("CREATE DATABASE foo NOWAIT") {
    parsesTo[Statements](
      CreateDatabase(literal("foo"), IfExistsThrowError, NoOptions, NoWait()(pos), None, None, None)(pos)
    )
  }

  test("CREATE DATABASE `foo.bar`") {
    parsesTo[Statements](CreateDatabase(
      literal("foo.bar"),
      IfExistsThrowError,
      NoOptions,
      NoWait()(pos),
      None,
      None,
      None
    )(pos))
  }

  test("CREATE DATABASE foo.bar") {
    parsesIn[Statements] {
      case Cypher5 => _.toAstPositioned(
          CreateDatabase(
            NamespacedName(List("bar"), Some("foo"))(pos),
            IfExistsThrowError,
            NoOptions,
            NoWait()(pos),
            None,
            None,
            None
          )(pos)
        )
      case _ => _.toAstPositioned(
          CreateDatabase(
            NamespacedName(List("foo.bar"), None)(pos),
            IfExistsThrowError,
            NoOptions,
            NoWait()(pos),
            None,
            None,
            None
          )(pos)
        )
    }
  }

  test("CREATE DATABASE `foo-bar42`") {
    parsesTo[Statements](
      CreateDatabase(literal("foo-bar42"), IfExistsThrowError, NoOptions, NoWait()(pos), None, None, None)(pos)
    )
  }

  test("CREATE DATABASE `_foo-bar42`") {
    parsesTo[Statements](
      CreateDatabase(literal("_foo-bar42"), IfExistsThrowError, NoOptions, NoWait()(pos), None, None, None)(pos)
    )
  }

  test("CREATE DATABASE ``") {
    parsesTo[Statements](
      CreateDatabase(literal(""), IfExistsThrowError, NoOptions, NoWait()(pos), None, None, None)(pos)
    )
  }

  test("CREATE DATABASE foo IF NOT EXISTS") {
    parsesTo[Statements](CreateDatabase(
      literalFoo,
      IfExistsDoNothing,
      NoOptions,
      NoWait()(pos),
      None,
      None,
      None
    )(pos))
  }

  test("CREATE DATABASE foo IF NOT EXISTS WAIT 10 SECONDS") {
    parsesTo[Statements](CreateDatabase(
      literalFoo,
      IfExistsDoNothing,
      NoOptions,
      TimeoutAfter("10")(defaultPos),
      None,
      None,
      None
    )(pos))
  }

  test("CREATE DATABASE foo IF NOT EXISTS WAIT") {
    parsesTo[Statements](
      CreateDatabase(literalFoo, IfExistsDoNothing, NoOptions, IndefiniteWait()(defaultPos), None, None, None)(pos)
    )
  }

  test("CREATE  DATABASE foo IF NOT EXISTS NOWAIT") {
    parsesTo[Statements](CreateDatabase(
      literalFoo,
      IfExistsDoNothing,
      NoOptions,
      NoWait()(pos),
      None,
      None,
      None
    )(pos))
  }

  test("CREATE DATABASE `_foo-bar42` IF NOT EXISTS") {
    parsesTo[Statements](
      CreateDatabase(literal("_foo-bar42"), IfExistsDoNothing, NoOptions, NoWait()(pos), None, None, None)(pos)
    )
  }

  test("CREATE OR REPLACE DATABASE foo") {
    parsesTo[Statements](CreateDatabase(literalFoo, IfExistsReplace, NoOptions, NoWait()(pos), None, None, None)(pos))
  }

  test("CREATE OR REPLACE DATABASE foo WAIT 10 SECONDS") {
    parsesTo[Statements](
      CreateDatabase(literalFoo, IfExistsReplace, NoOptions, TimeoutAfter("10")(defaultPos), None, None, None)(pos)
    )
  }

  test("CREATE OR REPLACE DATABASE foo WAIT") {
    parsesTo[Statements](
      CreateDatabase(literalFoo, IfExistsReplace, NoOptions, IndefiniteWait()(defaultPos), None, None, None)(pos)
    )
  }

  test("CREATE OR REPLACE DATABASE foo NOWAIT") {
    parsesTo[Statements](CreateDatabase(literalFoo, IfExistsReplace, NoOptions, NoWait()(pos), None, None, None)(pos))
  }

  test("CREATE OR REPLACE DATABASE `_foo-bar42`") {
    parsesTo[Statements](
      CreateDatabase(literal("_foo-bar42"), IfExistsReplace, NoOptions, NoWait()(pos), None, None, None)(pos)
    )
  }

  test("CREATE OR REPLACE DATABASE foo IF NOT EXISTS") {
    parsesTo[Statements](
      CreateDatabase(literalFoo, IfExistsInvalidSyntax, NoOptions, NoWait()(pos), None, None, None)(pos)
    )
  }

  test(
    "CREATE DATABASE foo OPTIONS {existingData: 'use', existingDataSeedInstance: '84c3ee6f-260e-47db-a4b6-589c807f2c2e'}"
  ) {
    assertAst(
      CreateDatabase(
        NamespacedName("foo")((1, 17, 16)),
        IfExistsThrowError,
        OptionsMap(Map(
          "existingData" -> StringLiteral("use")((1, 44, 43).withInputLength(5)),
          "existingDataSeedInstance" -> StringLiteral("84c3ee6f-260e-47db-a4b6-589c807f2c2e")(
            (1, 77, 76).withInputLength(38)
          )
        ))(pos),
        NoWait()(pos),
        None,
        None,
        None
      )(defaultPos)
    )
  }

  test(
    "CREATE DATABASE foo OPTIONS {existingData: 'use', existingDataSeedInstance: '84c3ee6f-260e-47db-a4b6-589c807f2c2e'} WAIT"
  ) {
    assertAst(
      CreateDatabase(
        NamespacedName("foo")((1, 17, 16)),
        IfExistsThrowError,
        OptionsMap(Map(
          "existingData" -> StringLiteral("use")((1, 44, 43).withInputLength(5)),
          "existingDataSeedInstance" -> StringLiteral("84c3ee6f-260e-47db-a4b6-589c807f2c2e")(
            (1, 77, 76).withInputLength(38)
          )
        ))(pos),
        IndefiniteWait()(pos),
        None,
        None,
        None
      )(defaultPos)
    )
  }

  test("CREATE DATABASE foo OPTIONS $param") {
    assertAst(
      CreateDatabase(
        NamespacedName("foo")((1, 17, 16)),
        IfExistsThrowError,
        OptionsParam(ExplicitParameter("param", CTMap)((1, 29, 28)))(pos),
        NoWait()(pos),
        None,
        None,
        None
      )(defaultPos)
    )
  }

  test("CREATE DATABASE foo TOPOLOGY 1 PRIMARY") {
    assertAst(
      CreateDatabase(
        literalFoo,
        IfExistsThrowError,
        NoOptions,
        NoWait()(pos),
        Some(Topology(Some(Left(1)), None)),
        None,
        None
      )(pos)
    )
  }

  test("CREATE DATABASE foo TOPOLOGY 1 PRIMARIES") {
    assertAst(
      CreateDatabase(
        literalFoo,
        IfExistsThrowError,
        NoOptions,
        NoWait()(pos),
        Some(Topology(Some(Left(1)), None)),
        None,
        None
      )(pos)
    )
  }

  test("CREATE DATABASE foo SET TOPOLOGY 1 PRIMARIES") {
    parsesIn[Statements] {
      case Cypher5 => _.withSyntaxErrorContaining(
          "Invalid input 'SET': expected a database name, 'DEFAULT LANGUAGE CYPHER', 'IF NOT EXISTS', 'NOWAIT', 'OPTIONS', 'TOPOLOGY', 'WAIT' or <EOF>"
        )
      case _ => _.toAstPositioned(CreateDatabase(
          literalFoo,
          IfExistsThrowError,
          NoOptions,
          NoWait()(pos),
          Some(Topology(Some(Left(1)), None)),
          None,
          None
        )(pos))
    }
  }

  test("CREATE DATABASE foo TOPOLOGY 1 PRIMARY 1 SECONDARY") {
    assertAst(
      CreateDatabase(
        literalFoo,
        IfExistsThrowError,
        NoOptions,
        NoWait()(pos),
        Some(Topology(Some(Left(1)), Some(Left(1)))),
        None,
        None
      )(pos)
    )
  }

  test("CREATE DATABASE foo TOPOLOGY 1 PRIMARY 2 SECONDARIES") {
    assertAst(
      CreateDatabase(
        literalFoo,
        IfExistsThrowError,
        NoOptions,
        NoWait()(pos),
        Some(Topology(Some(Left(1)), Some(Left(2)))),
        None,
        None
      )(pos)
    )
  }

  test("CREATE DATABASE foo TOPOLOGY 1 SECONDARY 1 PRIMARY") {
    assertAst(
      CreateDatabase(
        literalFoo,
        IfExistsThrowError,
        NoOptions,
        NoWait()(pos),
        Some(Topology(Some(Left(1)), Some(Left(1)))),
        None,
        None
      )(pos)
    )
  }

  test("CREATE DATABASE foo SET TOPOLOGY 1 SECONDARY 1 PRIMARY") {
    parsesIn[Statements] {
      case Cypher5 => _.withSyntaxErrorContaining(
          "Invalid input 'SET': expected a database name, 'DEFAULT LANGUAGE CYPHER', 'IF NOT EXISTS', 'NOWAIT', 'OPTIONS', 'TOPOLOGY', 'WAIT' or <EOF>"
        )
      case _ => _.toAstPositioned(CreateDatabase(
          literalFoo,
          IfExistsThrowError,
          NoOptions,
          NoWait()(pos),
          Some(Topology(Some(Left(1)), Some(Left(1)))),
          None,
          None
        )(pos))
    }
  }

  test("CREATE DATABASE foo TOPOLOGY 1 SECONDARY") {
    assertAst(
      CreateDatabase(
        literalFoo,
        IfExistsThrowError,
        NoOptions,
        NoWait()(pos),
        Some(Topology(None, Some(Left(1)))),
        None,
        None
      )(pos)
    )
  }

  test("CREATE DATABASE foo TOPOLOGY $param PRIMARY") {
    assertAst(
      CreateDatabase(
        literalFoo,
        IfExistsThrowError,
        NoOptions,
        NoWait()(pos),
        Some(Topology(Some(Right(intParam("param"))), None)),
        None,
        None
      )(pos)
    )
  }

  test("CREATE DATABASE foo TOPOLOGY $param PRIMARIES") {
    assertAst(
      CreateDatabase(
        literalFoo,
        IfExistsThrowError,
        NoOptions,
        NoWait()(pos),
        Some(Topology(Some(Right(intParam("param"))), None)),
        None,
        None
      )(pos)
    )
  }

  test("CREATE DATABASE foo TOPOLOGY $param PRIMARY $param2 SECONDARY") {
    assertAst(
      CreateDatabase(
        literalFoo,
        IfExistsThrowError,
        NoOptions,
        NoWait()(pos),
        Some(Topology(Some(Right(intParam("param"))), Some(Right(intParam("param2"))))),
        None,
        None
      )(pos)
    )
  }

  test("CREATE DATABASE foo TOPOLOGY 1 PRIMARY $param SECONDARIES") {
    assertAst(
      CreateDatabase(
        literalFoo,
        IfExistsThrowError,
        NoOptions,
        NoWait()(pos),
        Some(Topology(Some(Left(1)), Some(Right(intParam("param"))))),
        None,
        None
      )(pos)
    )
  }

  test("CREATE DATABASE foo TOPOLOGY $param SECONDARY $param2 PRIMARY") {
    assertAst(
      CreateDatabase(
        literalFoo,
        IfExistsThrowError,
        NoOptions,
        NoWait()(pos),
        Some(Topology(Some(Right(intParam("param2"))), Some(Right(intParam("param"))))),
        None,
        None
      )(pos)
    )
  }

  test("CREATE DATABASE foo TOPOLOGY $param SECONDARY") {
    assertAst(
      CreateDatabase(
        literalFoo,
        IfExistsThrowError,
        NoOptions,
        NoWait()(pos),
        Some(Topology(None, Some(Right(intParam("param"))))),
        None,
        None
      )(pos)
    )
  }

  test("CREATE DATABASE alias") {
    parsesTo[Statements](
      CreateDatabase(literal("alias"), IfExistsThrowError, NoOptions, NoWait()(pos), None, None, None)(pos)
    )
  }

  test("CREATE DATABASE alias IF NOT EXISTS") {
    parsesTo[Statements](
      CreateDatabase(literal("alias"), IfExistsDoNothing, NoOptions, NoWait()(pos), None, None, None)(pos)
    )
  }

  test("CREATE DATABASE") {
    // missing db name but parses as 'normal' cypher CREATE...
    failsParsing[Statements].withSyntaxError(
      """Invalid input '': expected a database name, a graph pattern or a parameter (line 1, column 16 (offset: 15))
        |"CREATE DATABASE"
        |                ^""".stripMargin
    )
  }

  test("CREATE DATABASE `graph.db`.`db.db`") {
    failsParsing[Statements].in {
      case Cypher5 => _.withMessageStart(
          "Invalid input ``graph.db`.`db.db`` for database name. Expected name to contain at most one component"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_22N05,
              "error: data exception - input failed validation. Invalid input '`graph.db`.`db.db`' for database name."
            )
              .withCause(
                GqlStatusInfoCodes.STATUS_22N83,
                "error: data exception - input consists of too many components. Expected name to contain at most 1 components separated by '.'."
              )
          )

      case _ => _.withSyntaxError(
          """Incorrectly formatted graph reference '`graph.db`.`db.db`'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually. (line 1, column 17 (offset: 16))
            |"CREATE DATABASE `graph.db`.`db.db`"
            |                 ^""".stripMargin
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_42NAA,
              "error: syntax error or access rule violation - incorrectly formatted graph reference. Incorrectly formatted graph reference '`graph.db`.`db.db`'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually."
            )
          )
    }
  }

  // Default language

  test("CREATE DATABASE foo DEFAULT LANGUAGE CYPHER 5") {
    parsesTo[Statements](CreateDatabase(
      literalFoo,
      IfExistsThrowError,
      NoOptions,
      NoWait()(pos),
      None,
      Some(org.neo4j.cypher.internal.CypherVersion.Cypher5),
      None
    )(pos))
  }

  test("CREATE DATABASE foo DEFAULT LANGUAGE CYPHER 25") {
    parsesTo[Statements](CreateDatabase(
      literalFoo,
      IfExistsThrowError,
      NoOptions,
      NoWait()(pos),
      None,
      Some(org.neo4j.cypher.internal.CypherVersion.Cypher25),
      None
    )(pos))
  }

  test("CREATE DATABASE foo SET DEFAULT LANGUAGE CYPHER 25") {
    parsesIn[Statements] {
      case Cypher5 => _.withSyntaxErrorContaining(
          "Invalid input 'SET': expected a database name, 'DEFAULT LANGUAGE CYPHER', 'IF NOT EXISTS', 'NOWAIT', 'OPTIONS', 'TOPOLOGY', 'WAIT' or <EOF>"
        )
      case _ => _.toAstPositioned(CreateDatabase(
          literalFoo,
          IfExistsThrowError,
          NoOptions,
          NoWait()(pos),
          None,
          Some(org.neo4j.cypher.internal.CypherVersion.Cypher25),
          None
        )(pos))
    }
  }

  test("CREATE DATABASE foo IF NOT EXISTS DEFAULT LANGUAGE CYPHER 25 OPTIONS { txLogEnrichment:'FULL' }") {
    parsesIn[Statement] {
      case Cypher5 => _.toAst(
          CreateDatabase(
            literalFoo,
            IfExistsDoNothing,
            OptionsMap(Map("txLogEnrichment" -> literalString("FULL")))(defaultPos),
            NoWait()(pos),
            None,
            Some(org.neo4j.cypher.internal.CypherVersion.Cypher25),
            None
          )(pos)
        )
      case _ => _.toAst(
          CreateDatabase(
            literalFoo,
            IfExistsDoNothing,
            OptionsMap(Map("txLogEnrichment" -> literalString("FULL")))(pos),
            NoWait()(pos),
            None,
            Some(org.neo4j.cypher.internal.CypherVersion.Cypher25),
            None
          )(pos)
        )
    }
  }

  test("CREATE DATABASE foo DEFAULT LANGUAGE CYPHER 25 TOPOLOGY 1 PRIMARY WAIT") {
    parsesTo[Statements](CreateDatabase(
      literalFoo,
      IfExistsThrowError,
      NoOptions,
      IndefiniteWait()(defaultPos),
      Some(Topology(Some(Left(1)), None)),
      Some(org.neo4j.cypher.internal.CypherVersion.Cypher25),
      None
    )(pos))
  }

  test("CREATE DATABASE foo SET DEFAULT LANGUAGE CYPHER 25 SET TOPOLOGY 1 PRIMARY") {
    parsesIn[Statements] {
      case Cypher5 => _.withSyntaxErrorContaining(
          "Invalid input 'SET': expected a database name, 'DEFAULT LANGUAGE CYPHER', 'IF NOT EXISTS', 'NOWAIT', 'OPTIONS', 'TOPOLOGY', 'WAIT' or <EOF>"
        )
      case _ => _.toAstPositioned(CreateDatabase(
          literalFoo,
          IfExistsThrowError,
          NoOptions,
          NoWait()(pos),
          Some(Topology(Some(Left(1)), None)),
          Some(org.neo4j.cypher.internal.CypherVersion.Cypher25),
          None
        )(pos))
    }
  }

  test("CREATE OR REPLACE DATABASE foo DEFAULT LANGUAGE CYPHER 25 ") {
    parsesTo[Statements](CreateDatabase(
      literalFoo,
      IfExistsReplace,
      NoOptions,
      NoWait()(pos),
      None,
      Some(org.neo4j.cypher.internal.CypherVersion.Cypher25),
      None
    )(pos))
  }

  // Negative tests

  test("CREATE DATABASE \"foo.bar\"") {
    failsParsing[Statements]
  }

  test("CREATE DATABASE foo-bar42") {
    failsParsing[Statements]
  }

  test("CREATE DATABASE _foo-bar42") {
    failsParsing[Statements]
  }

  test("CREATE DATABASE 42foo-bar") {
    failsParsing[Statements]
  }

  test("CREATE DATABASE 42foo") {
    failsParsing[Statements]
  }

  test("CREATE DATABASE _foo-bar42 IF NOT EXISTS") {
    failsParsing[Statements]
  }

  test("CREATE DATABASE `foo`.`bar`.`baz`") {
    failsParsing[Statements].in {
      case Cypher5 => _.withMessageStart(
          "Invalid input ``foo`.`bar`.`baz`` for database name. Expected name to contain at most one component"
        )
          .withSyntaxErrorGqlStatus(
            gqlStatus(
              GqlStatusInfoCodes.STATUS_22N05,
              "error: data exception - input failed validation. Invalid input '`foo`.`bar`.`baz`' for database name."
            )
              .withCause(
                GqlStatusInfoCodes.STATUS_22N83,
                "error: data exception - input consists of too many components. Expected name to contain at most 1 components separated by '.'."
              )
          )
      case _ => _.withSyntaxError(
          """Incorrectly formatted graph reference '`foo`.`bar`.`baz`'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually. (line 1, column 17 (offset: 16))
            |"CREATE DATABASE `foo`.`bar`.`baz`"
            |                 ^""".stripMargin
        ).withSyntaxErrorGqlStatus(
          gqlStatus(
            GqlStatusInfoCodes.STATUS_42NAA,
            "error: syntax error or access rule violation - incorrectly formatted graph reference. Incorrectly formatted graph reference '`foo`.`bar`.`baz`'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually."
          )
        )
    }
  }

  test("CREATE DATABASE  IF NOT EXISTS") {
    failsParsing[Statements].in {
      case Cypher5 =>
        _.withSyntaxError(
          """Invalid input 'NOT': expected a database name, 'DEFAULT LANGUAGE CYPHER', 'IF NOT EXISTS', 'NOWAIT', 'OPTIONS', 'TOPOLOGY', 'WAIT' or <EOF> (line 1, column 21 (offset: 20))
            |"CREATE DATABASE  IF NOT EXISTS"
            |                     ^""".stripMargin
        )
      case _ =>
        _.withSyntaxError(
          """Invalid input 'NOT': expected a database name, 'DEFAULT LANGUAGE CYPHER', 'IF NOT EXISTS', 'NOWAIT', 'OPTIONS', 'PROPERTY', 'SET', 'GRAPH SHARD', 'TOPOLOGY', 'WAIT' or <EOF> (line 1, column 21 (offset: 20))
            |"CREATE DATABASE  IF NOT EXISTS"
            |                     ^""".stripMargin
        )
    }
  }

  test("CREATE DATABASE foo IF EXISTS") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input 'EXISTS': expected 'NOT EXISTS' (line 1, column 24 (offset: 23))
        |"CREATE DATABASE foo IF EXISTS"
        |                        ^""".stripMargin
    )
  }

  test("CREATE DATABASE foo WAIT -12") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input '-': expected <EOF> or an integer value (line 1, column 26 (offset: 25))
        |"CREATE DATABASE foo WAIT -12"
        |                          ^""".stripMargin
    )
  }

  test("CREATE DATABASE foo WAIT 3.14") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input '3.14': expected <EOF> or an integer value (line 1, column 26 (offset: 25))
        |"CREATE DATABASE foo WAIT 3.14"
        |                          ^""".stripMargin
    )
  }

  test("CREATE DATABASE foo WAIT bar") {
    failsParsing[Statements]
  }

  test("CREATE OR REPLACE DATABASE _foo-bar42") {
    failsParsing[Statements]
  }

  test("CREATE OR REPLACE DATABASE") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input '': expected a database name or a parameter (line 1, column 27 (offset: 26))
        |"CREATE OR REPLACE DATABASE"
        |                           ^""".stripMargin
    )
  }

  test("CREATE DATABASE foo SET OPTION key value") {
    failsParsing[Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input 'SET': expected a database name, 'DEFAULT LANGUAGE CYPHER', 'IF NOT EXISTS', 'NOWAIT', 'OPTIONS', 'TOPOLOGY', 'WAIT' or <EOF> (line 1, column 21 (offset: 20))
            |"CREATE DATABASE foo SET OPTION key value"
            |                     ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input 'OPTION': expected 'DEFAULT LANGUAGE CYPHER', 'PROPERTY', 'GRAPH SHARD' or 'TOPOLOGY' (line 1, column 25 (offset: 24))
            |"CREATE DATABASE foo SET OPTION key value"
            |                         ^""".stripMargin
        )
    }
  }

  test("CREATE DATABASE foo OPTION {key: value}") {
    failsParsing[Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input 'OPTION': expected a database name, 'DEFAULT LANGUAGE CYPHER', 'IF NOT EXISTS', 'NOWAIT', 'OPTIONS', 'TOPOLOGY', 'WAIT' or <EOF> (line 1, column 21 (offset: 20))
            |"CREATE DATABASE foo OPTION {key: value}"
            |                     ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input 'OPTION': expected a database name, 'DEFAULT LANGUAGE CYPHER', 'IF NOT EXISTS', 'NOWAIT', 'OPTIONS', 'PROPERTY', 'SET', 'GRAPH SHARD', 'TOPOLOGY', 'WAIT' or <EOF> (line 1, column 21 (offset: 20))
            |"CREATE DATABASE foo OPTION {key: value}"
            |                     ^""".stripMargin
        )
    }
  }

  test("CREATE DATABASE foo SET OPTIONS key value") {
    failsParsing[Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input 'SET': expected a database name, 'DEFAULT LANGUAGE CYPHER', 'IF NOT EXISTS', 'NOWAIT', 'OPTIONS', 'TOPOLOGY', 'WAIT' or <EOF> (line 1, column 21 (offset: 20))
            |"CREATE DATABASE foo SET OPTIONS key value"
            |                     ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input 'OPTIONS': expected 'DEFAULT LANGUAGE CYPHER', 'PROPERTY', 'GRAPH SHARD' or 'TOPOLOGY' (line 1, column 25 (offset: 24))
            |"CREATE DATABASE foo SET OPTIONS key value"
            |                         ^""".stripMargin
        )
    }
  }

  test("CREATE DATABASE foo OPTIONS key value") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input 'key': expected a parameter or '{' (line 1, column 29 (offset: 28))
        |"CREATE DATABASE foo OPTIONS key value"
        |                             ^""".stripMargin
    )
  }

  test("CREATE DATABASE foo SET") {
    failsParsing[Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input 'SET': expected a database name, 'DEFAULT LANGUAGE CYPHER', 'IF NOT EXISTS', 'NOWAIT', 'OPTIONS', 'TOPOLOGY', 'WAIT' or <EOF> (line 1, column 21 (offset: 20))
            |"CREATE DATABASE foo SET"
            |                     ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input '': expected 'DEFAULT LANGUAGE CYPHER', 'PROPERTY', 'GRAPH SHARD' or 'TOPOLOGY' (line 1, column 24 (offset: 23))
            |"CREATE DATABASE foo SET"
            |                        ^""".stripMargin
        )
    }
  }

  test("CREATE DATABASE foo TOPOLOGY 1 PRIMARY TOPOLOGY 1 SECONDARY") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input 'TOPOLOGY': expected a parameter, 'NOWAIT', 'OPTIONS', 'WAIT', <EOF> or an integer value (line 1, column 40 (offset: 39))
        |"CREATE DATABASE foo TOPOLOGY 1 PRIMARY TOPOLOGY 1 SECONDARY"
        |                                        ^""".stripMargin
    )
  }

  test("CREATE DATABASE foo TOPOLOGY 1 PRIMARY 1 PRIMARY") {
    failsParsing[Statements].withSyntaxErrorContaining(
      """Duplicate PRIMARY clause (line 1, column 40 (offset: 39))""".stripMargin
    )
  }

  test("CREATE DATABASE foo TOPOLOGY 2 PRIMARIES 1 PRIMARY") {
    failsParsing[Statements].withSyntaxErrorContaining(
      """Duplicate PRIMARY clause (line 1, column 42 (offset: 41))"""
    )
  }

  test("CREATE DATABASE foo TOPOLOGY 2 SECONDARIES 1 SECONDARY") {
    failsParsing[Statements].withSyntaxErrorContaining(
      """Duplicate SECONDARY clause (line 1, column 44 (offset: 43))""".stripMargin
    )
  }

  test("CREATE DATABASE foo TOPOLOGY 1 PRIMARY 1 SECONDARY 2 SECONDARY") {
    failsParsing[Statements].withSyntaxErrorContaining(
      """Duplicate SECONDARY clause (line 1, column 52 (offset: 51))""".stripMargin
    )
  }

  test("CREATE DATABASE foo TOPOLOGY -1 PRIMARY") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input '-': expected a parameter or an integer value (line 1, column 30 (offset: 29))
        |"CREATE DATABASE foo TOPOLOGY -1 PRIMARY"
        |                              ^""".stripMargin
    )
  }

  test("CREATE DATABASE foo TOPOLOGY 1 PRIMARY -1 SECONDARY") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input '-': expected a parameter, 'NOWAIT', 'OPTIONS', 'WAIT', <EOF> or an integer value (line 1, column 40 (offset: 39))
        |"CREATE DATABASE foo TOPOLOGY 1 PRIMARY -1 SECONDARY"
        |                                        ^""".stripMargin
    )
  }

  test("CREATE DATABASE foo TOPOLOGY -1 SECONDARY 1 PRIMARY") {
    // Modify update error message. -1 is an integer...
    failsParsing[Statements].withSyntaxError(
      """Invalid input '-': expected a parameter or an integer value (line 1, column 30 (offset: 29))
        |"CREATE DATABASE foo TOPOLOGY -1 SECONDARY 1 PRIMARY"
        |                              ^""".stripMargin
    )
  }

  test("CREATE DATABASE foo TOPOLOGY 1 SECONDARY 1 SECONDARY") {
    failsParsing[Statements].withSyntaxErrorContaining(
      """Duplicate SECONDARY clause (line 1, column 42 (offset: 41))"""
    )
  }

  test("CREATE DATABASE foo TOPOLOGY") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input '': expected a parameter or an integer value (line 1, column 29 (offset: 28))
        |"CREATE DATABASE foo TOPOLOGY"
        |                             ^""".stripMargin
    )
  }

  test("CREATE DATABASE foo DEFAULT") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input '': expected 'LANGUAGE CYPHER' (line 1, column 28 (offset: 27))
        |"CREATE DATABASE foo DEFAULT"
        |                            ^""".stripMargin
    )
  }

  test("CREATE DATABASE foo DEFAULT LANGUAGE") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input '': expected 'CYPHER' (line 1, column 37 (offset: 36))
        |"CREATE DATABASE foo DEFAULT LANGUAGE"
        |                                     ^""".stripMargin
    )
  }

  test("CREATE DATABASE foo DEFAULT LANGUAGE CYPHER") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input '': expected an integer value (line 1, column 44 (offset: 43))
        |"CREATE DATABASE foo DEFAULT LANGUAGE CYPHER"
        |                                            ^""".stripMargin
    )
  }

  test("CREATE DATABASE foo DEFAULT LANGUAGE CYPHER 92") {
    failsParsing[Statements]
      .withSyntaxErrorGqlStatus(gqlStatus(
        GqlStatusInfoCodes.STATUS_22N04,
        "error: data exception - invalid input value. Invalid input '92' for Cypher version. Expected 'CYPHER 5' or 'CYPHER 25'."
      ))
      .withSyntaxError(
        """Invalid Cypher version '92'. Valid Cypher versions are: 5, 25 (line 1, column 45 (offset: 44))
          |"CREATE DATABASE foo DEFAULT LANGUAGE CYPHER 92"
          |                                             ^""".stripMargin
      )
  }

  test("CREATE DATABASE foo DEFAULT LANGUAGE CYPHER $param") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input '$': expected an integer value (line 1, column 45 (offset: 44))
        |"CREATE DATABASE foo DEFAULT LANGUAGE CYPHER $param"
        |                                             ^""".stripMargin
    )
  }

  // If you take the view that the version number is a Cypher number, then this should probably pass, but
  // we treat it as a word here, as it's part of the name.
  test("CREATE DATABASE foo DEFAULT LANGUAGE CYPHER 2_5") {
    failsParsing[Statements]
      .withSyntaxErrorGqlStatus(gqlStatus(
        GqlStatusInfoCodes.STATUS_22N04,
        "error: data exception - invalid input value. Invalid input '2_5' for Cypher version. Expected 'CYPHER 5' or 'CYPHER 25'."
      ))
      .withSyntaxError(
        """Invalid Cypher version '2_5'. Valid Cypher versions are: 5, 25 (line 1, column 45 (offset: 44))
          |"CREATE DATABASE foo DEFAULT LANGUAGE CYPHER 2_5"
          |                                             ^""".stripMargin
      )
  }

  test("CREATE DATABASE foo DEFAULT LANGUAGE CYPHER 24 + 1") {
    failsParsing[Statements]
      .withSyntaxErrorGqlStatus(gqlStatus(
        GqlStatusInfoCodes.STATUS_22N04,
        "error: data exception - invalid input value. Invalid input '24' for Cypher version. Expected 'CYPHER 5' or 'CYPHER 25'."
      ))
      .withSyntaxError(
        """Invalid Cypher version '24'. Valid Cypher versions are: 5, 25 (line 1, column 45 (offset: 44))
          |"CREATE DATABASE foo DEFAULT LANGUAGE CYPHER 24 + 1"
          |                                             ^""".stripMargin
      )
  }

  test("CREATE DATABASE foo DEFAULT LANGUAGE CYPHER 25 - 0") {
    failsParsing[Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input '-': expected 'NOWAIT', 'OPTIONS', 'TOPOLOGY', 'WAIT' or <EOF> (line 1, column 48 (offset: 47))
            |"CREATE DATABASE foo DEFAULT LANGUAGE CYPHER 25 - 0"
            |                                                ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input '-': expected 'NOWAIT', 'OPTIONS', 'PROPERTY', 'SET', 'GRAPH SHARD', 'TOPOLOGY', 'WAIT' or <EOF> (line 1, column 48 (offset: 47))
            |"CREATE DATABASE foo DEFAULT LANGUAGE CYPHER 25 - 0"
            |                                                ^""".stripMargin
        )
    }
  }

  test("CREATE DATABASE foo DEFAULT LANGUAGE CYPHER 9_2t") {
    failsParsing[Statements]
      .withSyntaxErrorGqlStatus(gqlStatus(
        GqlStatusInfoCodes.STATUS_22N04,
        "error: data exception - invalid input value. Invalid input '9_2t' for Cypher version. Expected 'CYPHER 5' or 'CYPHER 25'."
      ))
      .withSyntaxError(
        """Invalid Cypher version '9_2t'. Valid Cypher versions are: 5, 25 (line 1, column 45 (offset: 44))
          |"CREATE DATABASE foo DEFAULT LANGUAGE CYPHER 9_2t"
          |                                             ^""".stripMargin
      )
  }

  test("CREATE DATABASE foo GRAPH SHARD {}") {
    failsParsing[Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input 'GRAPH': expected a database name, 'DEFAULT LANGUAGE CYPHER', 'IF NOT EXISTS', 'NOWAIT', 'OPTIONS', 'TOPOLOGY', 'WAIT' or <EOF> (line 1, column 21 (offset: 20))
            |"CREATE DATABASE foo GRAPH SHARD {}"
            |                     ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input '': expected 'PROPERTY' (line 1, column 35 (offset: 34))
            |"CREATE DATABASE foo GRAPH SHARD {}"
            |                                   ^""".stripMargin
        )
    }
  }

  test("CREATE DATABASE foo GRAPH SHARD {TOPOLOGY 1 SECONDARY 2 SECONDARIES} PROPERTY SHARD {COUNT 1}") {
    failsParsing[Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input 'GRAPH': expected a database name, 'DEFAULT LANGUAGE CYPHER', 'IF NOT EXISTS', 'NOWAIT', 'OPTIONS', 'TOPOLOGY', 'WAIT' or <EOF> (line 1, column 21 (offset: 20))
            |"CREATE DATABASE foo GRAPH SHARD {TOPOLOGY 1 SECONDARY 2 SECONDARIES} PROPERTY SHARD {COUNT 1}"
            |                     ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Duplicate SECONDARY clause (line 1, column 55 (offset: 54))
            |"CREATE DATABASE foo GRAPH SHARD {TOPOLOGY 1 SECONDARY 2 SECONDARIES} PROPERTY SHARD {COUNT 1}"
            |                                                       ^""".stripMargin
        )
    }
  }

  test("CREATE DATABASE foo PROPERTY SHARD { COUNT -1 }") {
    failsParsing[Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input 'PROPERTY': expected a database name, 'DEFAULT LANGUAGE CYPHER', 'IF NOT EXISTS', 'NOWAIT', 'OPTIONS', 'TOPOLOGY', 'WAIT' or <EOF> (line 1, column 21 (offset: 20))
            |"CREATE DATABASE foo PROPERTY SHARD { COUNT -1 }"
            |                     ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input '-': expected an integer value (line 1, column 44 (offset: 43))
            |"CREATE DATABASE foo PROPERTY SHARD { COUNT -1 }"
            |                                            ^""".stripMargin
        )
    }
  }

  test("CREATE DATABASE foo PROPERTY SHARD { COUNT 1 TOPOLOGY -1 REPLICAS }") {
    failsParsing[Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input 'PROPERTY': expected a database name, 'DEFAULT LANGUAGE CYPHER', 'IF NOT EXISTS', 'NOWAIT', 'OPTIONS', 'TOPOLOGY', 'WAIT' or <EOF> (line 1, column 21 (offset: 20))
            |"CREATE DATABASE foo PROPERTY SHARD { COUNT 1 TOPOLOGY -1 REPLICAS }"
            |                     ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input '-': expected a parameter or an integer value (line 1, column 55 (offset: 54))
            |"CREATE DATABASE foo PROPERTY SHARD { COUNT 1 TOPOLOGY -1 REPLICAS }"
            |                                                       ^""".stripMargin
        )
    }
  }

  test("CREATE DATABASE foo PROPERTY SHARD { COUNT 1 TOPOLOGY 1 REPLICA 2 REPLICAS }") {
    failsParsing[Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input 'PROPERTY': expected a database name, 'DEFAULT LANGUAGE CYPHER', 'IF NOT EXISTS', 'NOWAIT', 'OPTIONS', 'TOPOLOGY', 'WAIT' or <EOF> (line 1, column 21 (offset: 20))
            |"CREATE DATABASE foo PROPERTY SHARD { COUNT 1 TOPOLOGY 1 REPLICA 2 REPLICAS }"
            |                     ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input '2': expected '}' (line 1, column 65 (offset: 64))
            |"CREATE DATABASE foo PROPERTY SHARD { COUNT 1 TOPOLOGY 1 REPLICA 2 REPLICAS }"
            |                                                                 ^""".stripMargin
        )
    }
  }
}
