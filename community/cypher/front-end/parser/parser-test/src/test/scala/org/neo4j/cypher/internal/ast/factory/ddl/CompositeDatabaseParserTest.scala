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

import org.neo4j.cypher.internal.ast.CascadeAliases
import org.neo4j.cypher.internal.ast.CreateCompositeDatabase
import org.neo4j.cypher.internal.ast.DestroyData
import org.neo4j.cypher.internal.ast.DropDatabase
import org.neo4j.cypher.internal.ast.DumpData
import org.neo4j.cypher.internal.ast.IfExistsDoNothing
import org.neo4j.cypher.internal.ast.IfExistsReplace
import org.neo4j.cypher.internal.ast.IfExistsThrowError
import org.neo4j.cypher.internal.ast.IndefiniteWait
import org.neo4j.cypher.internal.ast.NamespacedName
import org.neo4j.cypher.internal.ast.NoOptions
import org.neo4j.cypher.internal.ast.NoWait
import org.neo4j.cypher.internal.ast.OptionsMap
import org.neo4j.cypher.internal.ast.Restrict
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.TimeoutAfter
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5
import org.neo4j.cypher.internal.util.test_helpers.GqlExceptionMatchers.gqlStatus
import org.neo4j.gqlstatus.GqlStatusInfoCodes

class CompositeDatabaseParserTest extends AdministrationAndSchemaCommandParserTestBase {

  // create

  test("CREATE COMPOSITE DATABASE name") {
    parsesTo[Statements](
      CreateCompositeDatabase(namespacedName("name"), IfExistsThrowError, NoOptions, NoWait()(pos), None)(pos)
    )
  }

  test("CREATE COMPOSITE DATABASE $name") {
    parsesTo[Statements](
      CreateCompositeDatabase(stringParamName("name"), IfExistsThrowError, NoOptions, NoWait()(pos), None)(pos)
    )
  }

  test("CREATE COMPOSITE DATABASE `db.name`") {
    parsesTo[Statements](
      CreateCompositeDatabase(namespacedName("db.name"), IfExistsThrowError, NoOptions, NoWait()(pos), None)(pos)
    )
  }

  test("CREATE COMPOSITE DATABASE db.name") {
    parsesIn[Statements] {
      case Cypher5 => _.toAstPositioned(
          CreateCompositeDatabase(
            NamespacedName(List("name"), Some("db"))(pos),
            IfExistsThrowError,
            NoOptions,
            NoWait()(pos),
            None
          )(pos)
        )
      case _ => _.toAstPositioned(
          CreateCompositeDatabase(
            NamespacedName(List("db.name"), None)(pos),
            IfExistsThrowError,
            NoOptions,
            NoWait()(pos),
            None
          )(pos)
        )
    }
  }

  test("CREATE COMPOSITE DATABASE foo.bar") {
    parsesIn[Statements] {
      case Cypher5 => _.toAstPositioned(
          CreateCompositeDatabase(
            NamespacedName(List("bar"), Some("foo"))(pos),
            IfExistsThrowError,
            NoOptions,
            NoWait()(pos),
            None
          )(pos)
        )
      case _ => _.toAstPositioned(
          CreateCompositeDatabase(
            NamespacedName(List("foo.bar"), None)(pos),
            IfExistsThrowError,
            NoOptions,
            NoWait()(pos),
            None
          )(pos)
        )
    }
  }

  test("CREATE COMPOSITE DATABASE `graph.db`.`db.db`") {
    parsesIn[Statements] {
      // Fails in semantic checks instead
      case Cypher5 => _.toAstPositioned(
          CreateCompositeDatabase(
            namespacedName("graph.db", "db.db"),
            IfExistsThrowError,
            NoOptions,
            NoWait()(pos),
            None
          )(pos)
        )
      case _ =>
        _.withSyntaxError(
          """Incorrectly formatted graph reference '`graph.db`.`db.db`'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually. (line 1, column 27 (offset: 26))
            |"CREATE COMPOSITE DATABASE `graph.db`.`db.db`"
            |                           ^""".stripMargin
        )
    }
  }

  test("CREATE COMPOSITE DATABASE name IF NOT EXISTS") {
    parsesTo[Statements](CreateCompositeDatabase(
      namespacedName("name"),
      IfExistsDoNothing,
      NoOptions,
      NoWait()(pos),
      None
    )(pos))
  }

  test("CREATE OR REPLACE COMPOSITE DATABASE name") {
    parsesTo[Statements](CreateCompositeDatabase(
      namespacedName("name"),
      IfExistsReplace,
      NoOptions,
      NoWait()(pos),
      None
    )(pos))
  }

  test("CREATE COMPOSITE DATABASE name OPTIONS {}") {
    parsesIn[Statement] {
      case Cypher5 => _.toAst(
          CreateCompositeDatabase(
            namespacedName("name"),
            IfExistsThrowError,
            OptionsMap(Map.empty)(defaultPos),
            NoWait()(pos),
            None
          )(pos)
        )
      case _ => _.toAstPositioned(
          CreateCompositeDatabase(
            namespacedName("name"),
            IfExistsThrowError,
            OptionsMap(Map.empty)(pos),
            NoWait()(pos),
            None
          )(pos)
        )
    }
  }

  test("CREATE COMPOSITE DATABASE name OPTIONS {someKey: 'someValue'} NOWAIT") {
    parsesIn[Statement] {
      case Cypher5 => _.toAst(
          CreateCompositeDatabase(
            namespacedName("name"),
            IfExistsThrowError,
            OptionsMap(Map(
              "someKey" -> literalString("someValue")
            ))(defaultPos),
            NoWait()(pos),
            None
          )(pos)
        )
      case _ => _.toAstPositioned(
          CreateCompositeDatabase(
            namespacedName("name"),
            IfExistsThrowError,
            OptionsMap(Map(
              "someKey" -> literalString("someValue")
            ))(pos),
            NoWait()(pos),
            None
          )(pos)
        )
    }
  }

  test("CREATE COMPOSITE DATABASE name TOPOLOGY 1 PRIMARY") {
    failsParsing[Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input 'TOPOLOGY': expected a database name, 'DEFAULT LANGUAGE CYPHER', 'IF NOT EXISTS', 'NOWAIT', 'OPTIONS', 'WAIT' or <EOF> (line 1, column 32 (offset: 31))
            |"CREATE COMPOSITE DATABASE name TOPOLOGY 1 PRIMARY"
            |                                ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input 'TOPOLOGY': expected a database name, 'DEFAULT LANGUAGE CYPHER', 'IF NOT EXISTS', 'NOWAIT', 'OPTIONS', 'SET', 'WAIT' or <EOF> (line 1, column 32 (offset: 31))
            |"CREATE COMPOSITE DATABASE name TOPOLOGY 1 PRIMARY"
            |                                ^""".stripMargin
        )
    }
  }

  test("CREATE COMPOSITE DATABASE name SET TOPOLOGY 1 PRIMARY") {
    failsParsing[Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input 'SET': expected a database name, 'DEFAULT LANGUAGE CYPHER', 'IF NOT EXISTS', 'NOWAIT', 'OPTIONS', 'WAIT' or <EOF> (line 1, column 32 (offset: 31))
            |"CREATE COMPOSITE DATABASE name SET TOPOLOGY 1 PRIMARY"
            |                                ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input 'TOPOLOGY': expected 'DEFAULT LANGUAGE CYPHER' (line 1, column 36 (offset: 35))
            |"CREATE COMPOSITE DATABASE name SET TOPOLOGY 1 PRIMARY"
            |                                    ^""".stripMargin
        )
    }
  }

  test("CREATE COMPOSITE DATABASE name WAIT") {
    parsesTo[Statements](CreateCompositeDatabase(
      namespacedName("name"),
      IfExistsThrowError,
      NoOptions,
      IndefiniteWait()(defaultPos),
      None
    )(pos))
  }

  test("CREATE COMPOSITE DATABASE name NOWAIT") {
    parsesTo[Statements](
      CreateCompositeDatabase(namespacedName("name"), IfExistsThrowError, NoOptions, NoWait()(pos), None)(pos)
    )
  }

  test("CREATE COMPOSITE DATABASE name WAIT 10 SECONDS") {
    parsesTo[Statements](CreateCompositeDatabase(
      namespacedName("name"),
      IfExistsThrowError,
      NoOptions,
      TimeoutAfter("10")(defaultPos),
      None
    )(pos))
  }

  // Default language

  test("CREATE COMPOSITE DATABASE foo DEFAULT LANGUAGE CYPHER 5") {
    parsesTo[Statements](CreateCompositeDatabase(
      literalFoo,
      IfExistsThrowError,
      NoOptions,
      NoWait()(pos),
      Some(org.neo4j.cypher.internal.CypherVersion.Cypher5)
    )(pos))
  }

  test("CREATE COMPOSITE DATABASE foo DEFAULT LANGUAGE CYPHER 25") {
    parsesTo[Statements](CreateCompositeDatabase(
      literalFoo,
      IfExistsThrowError,
      NoOptions,
      NoWait()(pos),
      Some(org.neo4j.cypher.internal.CypherVersion.Cypher25)
    )(pos))
  }

  test("CREATE COMPOSITE DATABASE foo SET DEFAULT LANGUAGE CYPHER 25") {
    parsesIn[Statements] {
      case Cypher5 => _.withSyntaxErrorContaining(
          "Invalid input 'SET': expected a database name, 'DEFAULT LANGUAGE CYPHER', 'IF NOT EXISTS', 'NOWAIT', 'OPTIONS', 'WAIT' or <EOF>"
        )
      case _ => _.toAstPositioned(CreateCompositeDatabase(
          literalFoo,
          IfExistsThrowError,
          NoOptions,
          NoWait()(pos),
          Some(org.neo4j.cypher.internal.CypherVersion.Cypher25)
        )(pos))
    }
  }

  test("CREATE COMPOSITE DATABASE foo IF NOT EXISTS DEFAULT LANGUAGE CYPHER 25 WAIT") {
    parsesTo[Statements](CreateCompositeDatabase(
      literalFoo,
      IfExistsDoNothing,
      NoOptions,
      IndefiniteWait()(defaultPos),
      Some(org.neo4j.cypher.internal.CypherVersion.Cypher25)
    )(pos))
  }

  test("CREATE COMPOSITE DATABASE foo DEFAULT LANGUAGE CYPHER 25 WAIT") {
    parsesTo[Statements](CreateCompositeDatabase(
      literalFoo,
      IfExistsThrowError,
      NoOptions,
      IndefiniteWait()(defaultPos),
      Some(org.neo4j.cypher.internal.CypherVersion.Cypher25)
    )(pos))
  }

  test("CREATE COMPOSITE DATABASE foo DEFAULT LANGUAGE CYPHER 25 OPTIONS {someKey: 'someValue'} ") {
    parsesIn[Statement] {
      case Cypher5 => _.toAst(
          CreateCompositeDatabase(
            literalFoo,
            IfExistsThrowError,
            OptionsMap(Map(
              "someKey" -> literalString("someValue")
            ))(defaultPos),
            NoWait()(pos),
            Some(org.neo4j.cypher.internal.CypherVersion.Cypher25)
          )(pos)
        )
      case _ => _.toAstPositioned(
          CreateCompositeDatabase(
            literalFoo,
            IfExistsThrowError,
            OptionsMap(Map(
              "someKey" -> literalString("someValue")
            ))(pos),
            NoWait()(pos),
            Some(org.neo4j.cypher.internal.CypherVersion.Cypher25)
          )(pos)
        )
    }
  }

  test("CREATE COMPOSITE DATABASE foo OPTIONS {someKey: 'someValue'} DEFAULT LANGUAGE CYPHER 25") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input 'DEFAULT': expected 'NOWAIT', 'WAIT' or <EOF> (line 1, column 62 (offset: 61))
        |"CREATE COMPOSITE DATABASE foo OPTIONS {someKey: 'someValue'} DEFAULT LANGUAGE CYPHER 25"
        |                                                              ^""".stripMargin
    )
  }

  test("CREATE COMPOSITE DATABASE foo DEFAULT") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input '': expected 'LANGUAGE CYPHER' (line 1, column 38 (offset: 37))
        |"CREATE COMPOSITE DATABASE foo DEFAULT"
        |                                      ^""".stripMargin
    )
  }

  test("CREATE COMPOSITE DATABASE foo DEFAULT LANGUAGE") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input '': expected 'CYPHER' (line 1, column 47 (offset: 46))
        |"CREATE COMPOSITE DATABASE foo DEFAULT LANGUAGE"
        |                                               ^""".stripMargin
    )
  }

  test("CREATE COMPOSITE DATABASE foo DEFAULT LANGUAGE CYPHER") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input '': expected an integer value (line 1, column 54 (offset: 53))
        |"CREATE COMPOSITE DATABASE foo DEFAULT LANGUAGE CYPHER"
        |                                                      ^""".stripMargin
    )
  }

  test("CREATE COMPOSITE DATABASE foo DEFAULT LANGUAGE CYPHER 77") {
    failsParsing[Statements]
      .withSyntaxErrorGqlStatus(gqlStatus(
        GqlStatusInfoCodes.STATUS_22N04,
        "error: data exception - invalid input value. Invalid input '77' for Cypher version. Expected 'CYPHER 5' or 'CYPHER 25'."
      ))
      .withSyntaxError(
        """Invalid Cypher version '77'. Valid Cypher versions are: 5, 25 (line 1, column 55 (offset: 54))
          |"CREATE COMPOSITE DATABASE foo DEFAULT LANGUAGE CYPHER 77"
          |                                                       ^""".stripMargin
      )
  }

  // drop

  test("DROP COMPOSITE DATABASE name") {
    parsesTo[Statements](DropDatabase(
      namespacedName("name"),
      ifExists = false,
      composite = true,
      Restrict,
      DestroyData,
      NoWait()(pos)
    )(pos))
  }

  test("DROP COMPOSITE DATABASE `db.name`") {
    parsesTo[Statements](DropDatabase(
      namespacedName("db.name"),
      ifExists = false,
      composite = true,
      Restrict,
      DestroyData,
      NoWait()(pos)
    )(pos))
  }

  test("DROP COMPOSITE DATABASE db.name") {
    parsesIn[Statements] {
      case Cypher5 => _.toAstPositioned(
          DropDatabase(
            NamespacedName(List("name"), Some("db"))(pos),
            ifExists = false,
            composite = true,
            Restrict,
            DestroyData,
            NoWait()(pos)
          )(pos)
        )
      case _ => _.toAstPositioned(
          DropDatabase(
            NamespacedName(List("db.name"), None)(pos),
            ifExists = false,
            composite = true,
            Restrict,
            DestroyData,
            NoWait()(pos)
          )(pos)
        )
    }
  }

  test("DROP COMPOSITE DATABASE $name") {
    parsesTo[Statements](DropDatabase(
      stringParamName("name"),
      ifExists = false,
      composite = true,
      Restrict,
      DestroyData,
      NoWait()(pos)
    )(pos))
  }

  test("DROP COMPOSITE DATABASE name IF EXISTS") {
    parsesTo[Statements](DropDatabase(
      namespacedName("name"),
      ifExists = true,
      composite = true,
      Restrict,
      DestroyData,
      NoWait()(pos)
    )(pos))
  }

  test("DROP COMPOSITE DATABASE name WAIT") {
    parsesTo[Statements](DropDatabase(
      namespacedName("name"),
      ifExists = false,
      composite = true,
      Restrict,
      DestroyData,
      IndefiniteWait()(defaultPos)
    )(pos))
  }

  test("DROP COMPOSITE DATABASE name WAIT 10 SECONDS") {
    parsesTo[Statements](DropDatabase(
      namespacedName("name"),
      ifExists = false,
      composite = true,
      Restrict,
      DestroyData,
      TimeoutAfter("10")(defaultPos)
    )(pos))
  }

  test("DROP COMPOSITE DATABASE name NOWAIT") {
    parsesTo[Statements](DropDatabase(
      namespacedName("name"),
      ifExists = false,
      composite = true,
      Restrict,
      DestroyData,
      NoWait()(pos)
    )(pos))
  }

  test("DROP COMPOSITE DATABASE foo DUMP DATA") {
    parsesTo[Statements](
      DropDatabase(literalFoo, ifExists = false, composite = true, Restrict, DumpData, NoWait()(pos))(pos)
    )
  }

  test("DROP COMPOSITE DATABASE foo DESTROY DATA") {
    parsesTo[Statements](
      DropDatabase(literalFoo, ifExists = false, composite = true, Restrict, DestroyData, NoWait()(pos))(pos)
    )
  }

  test("DROP COMPOSITE DATABASE name RESTRICT") {
    parsesTo[Statements](DropDatabase(
      namespacedName("name"),
      ifExists = false,
      composite = true,
      Restrict,
      DestroyData,
      NoWait()(pos)
    )(pos))
  }

  test("DROP COMPOSITE DATABASE name CASCADE ALIASES") {
    parsesTo[Statements](DropDatabase(
      namespacedName("name"),
      ifExists = false,
      composite = true,
      CascadeAliases,
      DestroyData,
      NoWait()(pos)
    )(pos))
  }

  test("DROP COMPOSITE DATABASE name IF EXISTS CASCADE ALIAS") {
    parsesTo[Statements](DropDatabase(
      namespacedName("name"),
      ifExists = true,
      composite = true,
      CascadeAliases,
      DestroyData,
      NoWait()(pos)
    )(pos))
  }

  test("DROP COMPOSITE DATABASE name RESTRICT DUMP DATA") {
    parsesTo[Statements](DropDatabase(
      namespacedName("name"),
      ifExists = false,
      composite = true,
      Restrict,
      DumpData,
      NoWait()(pos)
    )(pos))
  }

  test("DROP COMPOSITE DATABASE name CASCADE ALIASES WAIT") {
    parsesTo[Statements](DropDatabase(
      namespacedName("name"),
      ifExists = false,
      composite = true,
      CascadeAliases,
      DestroyData,
      IndefiniteWait()(defaultPos)
    )(pos))
  }
}
