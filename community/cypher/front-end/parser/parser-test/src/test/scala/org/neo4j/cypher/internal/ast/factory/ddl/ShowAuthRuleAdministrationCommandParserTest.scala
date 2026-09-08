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

import org.neo4j.cypher.internal.ast.ShowAuthRules
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5
import org.neo4j.cypher.internal.util.test_helpers.GqlExceptionMatchers.gqlStatus
import org.neo4j.gqlstatus.GqlStatusInfoCodes

class ShowAuthRuleAdministrationCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {

  test("SHOW AUTH RULES") {
    parsesIn[Statement] {
      case Cypher5 => _.withMessageStart("Invalid input 'AUTH'")
      case _       => _.toAst(ShowAuthRules(None, asCommands = false)(pos))
    }
  }

  test("SHOW AUTH RULE") {
    parsesIn[Statement] {
      case Cypher5 => _.withMessageStart("Invalid input 'AUTH'")
      case _       => _.toAst(ShowAuthRules(None, asCommands = false)(pos))
    }
  }

  test("USE system SHOW AUTH RULES") {
    parsesIn[Statement] {
      case Cypher5 => _.withMessageStart("Invalid input 'AUTH'")
      case _ => _.toAst(ShowAuthRules(None, asCommands = false)(pos).withGraph(Some(
          use(List("system"), resolveStrictly = true)
        )))
    }
  }

  test("SHOW AUTH RULES WHERE name = 'GRANTED'") {
    parsesIn[Statement] {
      case Cypher5 => _.withMessageStart("Invalid input 'AUTH'")
      case _ =>
        _.toAst(ShowAuthRules(Some(Right(where(equals(varFor("name"), grantedString)))), asCommands = false)(pos))
    }
  }

  test("SHOW AUTH RULE WHERE name = 'GRANTED'") {
    parsesIn[Statement] {
      case Cypher5 => _.withMessageStart("Invalid input 'AUTH'")
      case _ =>
        _.toAst(ShowAuthRules(Some(Right(where(equals(varFor("name"), grantedString)))), asCommands = false)(pos))
    }
  }

  test("SHOW AUTH RULES WHERE name = 'GRANTED' AND condition = true") {
    val namePredicate = equals(varFor("name"), grantedString)
    val conditionPredicate = equals(varFor("condition"), literalBoolean(true))
    parsesIn[Statement] {
      case Cypher5 => _.withMessageStart("Invalid input 'AUTH'")
      case _ =>
        _.toAst(ShowAuthRules(Some(Right(where(and(namePredicate, conditionPredicate)))), asCommands = false)(pos))
    }
  }

  test("SHOW AUTH RULES WHERE name = 'GRANTED' OR condition = true") {
    val namePredicate = equals(varFor("name"), grantedString)
    val conditionPredicate = equals(varFor("condition"), literalBoolean(true))
    parsesIn[Statement] {
      case Cypher5 => _.withMessageStart("Invalid input 'AUTH'")
      case _ =>
        _.toAst(ShowAuthRules(Some(Right(where(or(namePredicate, conditionPredicate)))), asCommands = false)(pos))
    }
  }

  test("SHOW AUTH RULES YIELD name ORDER BY name") {
    val columns = yieldClause(returnItems(variableReturnItem("name")), Some(orderBy(sortItem(varFor("name")))))
    parsesIn[Statement] {
      case Cypher5 => _.withMessageStart("Invalid input 'AUTH'")
      case _       => _.toAst(ShowAuthRules(Some(Left((columns, None))), asCommands = false)(pos))
    }
  }

  test("SHOW AUTH RULES YIELD name") {
    val columns = yieldClause(returnItems(variableReturnItem("name")), None)
    parsesIn[Statement] {
      case Cypher5 => _.withMessageStart("Invalid input 'AUTH'")
      case _       => _.toAst(ShowAuthRules(Some(Left((columns, None))), asCommands = false)(pos))
    }
  }

  test("SHOW AUTH RULES YIELD name ORDER BY name WHERE name ='none'") {
    val orderByClause = orderBy(sortItem(varFor("name")))
    val whereClause = where(equals(varFor("name"), noneString))
    val columns =
      yieldClause(returnItems(variableReturnItem("name")), Some(orderByClause), where = Some(whereClause))
    parsesIn[Statement] {
      case Cypher5 => _.withMessageStart("Invalid input 'AUTH'")
      case _       => _.toAst(ShowAuthRules(Some(Left((columns, None))), asCommands = false)(pos))
    }
  }

  test("SHOW AUTH RULES YIELD name ORDER BY name SKIP 1 LIMIT 10 WHERE name ='none'") {
    val orderByClause = orderBy(sortItem(varFor("name")))
    val whereClause = where(equals(varFor("name"), noneString))
    val columns = yieldClause(
      returnItems(variableReturnItem("name")),
      Some(orderByClause),
      Some(skip(1)),
      Some(limit(10)),
      Some(whereClause)
    )
    parsesIn[Statement] {
      case Cypher5 => _.withMessageStart("Invalid input 'AUTH'")
      case _       => _.toAst(ShowAuthRules(Some(Left((columns, None))), asCommands = false)(pos))
    }
  }

  test("SHOW AUTH RULES YIELD name ORDER BY name OFFSET 1 LIMIT 10 WHERE name ='none'") {
    val orderByClause = orderBy(sortItem(varFor("name")))
    val whereClause = where(equals(varFor("name"), noneString))
    val columns = yieldClause(
      returnItems(variableReturnItem("name")),
      Some(orderByClause),
      Some(skip(1)),
      Some(limit(10)),
      Some(whereClause)
    )
    parsesIn[Statement] {
      case Cypher5 => _.withMessageStart("Invalid input 'AUTH'")
      case _       => _.toAst(ShowAuthRules(Some(Left((columns, None))), asCommands = false)(pos))
    }
  }

  test("SHOW AUTH RULES YIELD name SKIP -1") {
    val columns = yieldClause(returnItems(variableReturnItem("name")), skip = Some(skip(-1)))
    parsesIn[Statement] {
      case Cypher5 => _.withMessageStart("Invalid input 'AUTH'")
      case _       => _.toAst(ShowAuthRules(Some(Left((columns, None))), asCommands = false)(pos))
    }
  }

  test("SHOW AUTH RULES YIELD name RETURN name ORDER BY name") {
    parsesIn[Statement] {
      case Cypher5 => _.withMessageStart("Invalid input 'AUTH'")
      case _ => _.toAst(ShowAuthRules(
          Some(Left((
            yieldClause(returnItems(variableReturnItem("name"))),
            Some(returnClause(returnItems(variableReturnItem("name")), Some(orderBy(sortItem(varFor("name"))))))
          ))),
          asCommands = false
        )(pos))
    }
  }

  test("SHOW AUTH RULES YIELD name, suspended as suspended WHERE suspended RETURN DISTINCT name") {
    val suspendedVar = varFor("suspended")
    parsesIn[Statement] {
      case Cypher5 => _.withMessageStart("Invalid input 'AUTH'")
      case _ => _.toAst(ShowAuthRules(
          Some(Left((
            yieldClause(
              returnItems(variableReturnItem("name"), aliasedReturnItem(suspendedVar)),
              where = Some(where(suspendedVar))
            ),
            Some(returnClause(returnItems(variableReturnItem("name")), distinct = true))
          ))),
          asCommands = false
        )(pos))
    }
  }

  test("SHOW AUTH RULES YIELD * RETURN *") {
    parsesIn[Statement] {
      case Cypher5 => _.withMessageStart("Invalid input 'AUTH'")
      case _ => _.toAst(ShowAuthRules(
          Some(Left((yieldClause(returnAllItems), Some(returnClause(returnAllItems))))),
          asCommands = false
        )(pos))
    }
  }

  test("SHOW AUTH RULES YIELD *") {
    parsesIn[Statement] {
      case Cypher5 => _.withMessageStart("Invalid input 'AUTH'")
      case _       => _.toAst(ShowAuthRules(Some(Left((yieldClause(returnAllItems), None))), asCommands = false)(pos))
    }
  }

  test("SHOW AUTH RULE YIELD *") {
    parsesIn[Statement] {
      case Cypher5 => _.withMessageStart("Invalid input 'AUTH'")
      case _       => _.toAst(ShowAuthRules(Some(Left((yieldClause(returnAllItems), None))), asCommands = false)(pos))
    }
  }

  test("SHOW AUTH RULES YIELD * ORDER BY name") {
    parsesIn[Statement] {
      case Cypher5 => _.withMessageStart("Invalid input 'AUTH'")
      case _ => _.toAst(ShowAuthRules(
          Some(Left((yieldClause(returnAllItems, Some(orderBy(sortItem(varFor("name"))))), None))),
          asCommands = false
        )(pos))
    }
  }

  test("SHOW AUTH RULES YIELD * ORDER BY name OFFSET 1 LIMIT 10 WHERE name ='none'") {
    val orderByClause = orderBy(sortItem(varFor("name")))
    val whereClause = where(equals(varFor("name"), noneString))
    val columns = yieldClause(
      returnAllItems,
      Some(orderByClause),
      Some(skip(1)),
      Some(limit(10)),
      Some(whereClause)
    )
    parsesIn[Statement] {
      case Cypher5 => _.withMessageStart("Invalid input 'AUTH'")
      case _       => _.toAst(ShowAuthRules(Some(Left((columns, None))), asCommands = false)(pos))
    }
  }

  test("SHOW AUTH RULES YIELD * SKIP -1") {
    val columns = yieldClause(returnAllItems, skip = Some(skip(-1)))
    parsesIn[Statement] {
      case Cypher5 => _.withMessageStart("Invalid input 'AUTH'")
      case _       => _.toAst(ShowAuthRules(Some(Left((columns, None))), asCommands = false)(pos))
    }
  }

  test("SHOW AUTH RULES AS COMMANDS") {
    parsesIn[Statement] {
      case Cypher5 => _.withMessageStart("Invalid input 'AUTH'")
      case _       => _.toAst(ShowAuthRules(None, asCommands = true)(pos))
    }
  }

  test("SHOW AUTH RULE AS COMMAND") {
    parsesIn[Statement] {
      case Cypher5 => _.withMessageStart("Invalid input 'AUTH'")
      case _       => _.toAst(ShowAuthRules(None, asCommands = true)(pos))
    }
  }

  test("USE system SHOW AUTH RULES AS COMMANDS") {
    parsesIn[Statement] {
      case Cypher5 => _.withMessageStart("Invalid input 'AUTH'")
      case _ => _.toAst(ShowAuthRules(None, asCommands = true)(pos).withGraph(Some(
          use(List("system"), resolveStrictly = true)
        )))
    }
  }

  test(
    "SHOW AUTH RULES AS COMMANDS WHERE command = 'CREATE AUTH RULE region SET CONDITION abac.oidc.user_attribute(‘region’) = ‘region1’ SET ENABLED true'"
  ) {
    parsesIn[Statement] {
      case Cypher5 => _.withMessageStart("Invalid input 'AUTH'")
      case _ =>
        _.toAst(ShowAuthRules(
          Some(Right(where(equals(
            varFor("command"),
            "CREATE AUTH RULE region SET CONDITION abac.oidc.user_attribute(‘region’) = ‘region1’ SET ENABLED true"
          )))),
          asCommands = true
        )(pos))
    }
  }

  test("SHOW AUTH RULES AS COMMANDS YIELD command ORDER BY command SKIP 1 LIMIT 10 WHERE command ='none'") {
    val orderByClause = orderBy(sortItem(varFor("command")))
    val whereClause = where(equals(varFor("command"), noneString))
    val columns = yieldClause(
      returnItems(variableReturnItem("command")),
      Some(orderByClause),
      Some(skip(1)),
      Some(limit(10)),
      Some(whereClause)
    )
    parsesIn[Statement] {
      case Cypher5 => _.withMessageStart("Invalid input 'AUTH'")
      case _       => _.toAst(ShowAuthRules(Some(Left((columns, None))), asCommands = true)(pos))
    }
  }

  test("SHOW AUTH RULE AS COMMANDS YIELD *") {
    parsesIn[Statement] {
      case Cypher5 => _.withMessageStart("Invalid input 'AUTH'")
      case _       => _.toAst(ShowAuthRules(Some(Left((yieldClause(returnAllItems), None))), asCommands = true)(pos))
    }
  }

  test("SHOW AUTH RULE AS COMMANDS YIELD * RETURN role") {
    parsesIn[Statement] {
      case Cypher5 => _.withMessageStart("Invalid input 'AUTH'")
      case _ => _.toAst(ShowAuthRules(
          Some(Left((yieldClause(returnAllItems), Some(returnClause(returnItems(variableReturnItem("role")), None))))),
          asCommands = true
        )(pos))
    }
  }

  test("SHOW AUTH RULE AS COMMANDS YIELD * RETURN role ORDER BY role") {
    parsesIn[Statement] {
      case Cypher5 => _.withMessageStart("Invalid input 'AUTH'")
      case _ => _.toAst(ShowAuthRules(
          Some(Left((
            yieldClause(returnAllItems),
            Some(returnClause(returnItems(variableReturnItem("role")), Some(orderBy(sortItem(varFor("role"))))))
          ))),
          asCommands = true
        )(pos))
    }
  }

  // fails parsing

  test("SHOW AUTH RULES YIELD *,blah RETURN name") {
    failsParsing[Statements]
  }

  test("SHOW AUTH RULES YIELD (123 + xyz)") {
    failsParsing[Statements]
  }

  test("SHOW AUTH RULES YIELD (123 + xyz) AS foo") {
    failsParsing[Statements]
  }

  test("SHOW AUTH RULES AS") {
    failsParsing[Statements]
  }

  test("SHOW AUTH RULES YIELD * AS COMMANDS") {
    failsParsing[Statements]
  }

  test("SHOW AUTH RULES WHERE name = 'rule' AS COMMANDS") {
    failsParsing[Statements]
  }

  test("SHOW AUTH RULES RETURN *") {
    failsParsing[Statements].in {
      case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'AUTH'")
      case _ => _.withSyntaxError(
          """Invalid input '*': expected 'PRIVILEGE' or 'PRIVILEGES' (line 1, column 24 (offset: 23))
            |"SHOW AUTH RULES RETURN *"
            |                        ^""".stripMargin
        ).withSyntaxErrorGqlStatus(
          gqlStatus(
            GqlStatusInfoCodes.STATUS_42I06,
            "error: syntax error or access rule violation - invalid input. Invalid input '*', expected: 'PRIVILEGE' or 'PRIVILEGES'."
          )
        )
    }
  }

  test("SHOW AUTH RULES RETURN") {
    failsParsing[Statements].in {
      case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'AUTH'")
      case _ => _.withSyntaxError(
          """Invalid input '': expected 'PRIVILEGE' or 'PRIVILEGES' (line 1, column 23 (offset: 22))
            |"SHOW AUTH RULES RETURN"
            |                       ^""".stripMargin
        ).withSyntaxErrorGqlStatus(
          gqlStatus(
            GqlStatusInfoCodes.STATUS_42I06,
            "error: syntax error or access rule violation - invalid input. Invalid input '', expected: 'PRIVILEGE' or 'PRIVILEGES'."
          )
        )
    }
  }

  test("SHOW AUTH RULES YIELD") {
    failsParsing[Statements].in {
      case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'AUTH'")
      case _ => _.withSyntaxError(
          """Invalid input '': expected a variable name, '*', ',', 'PRIVILEGE' or 'PRIVILEGES' (line 1, column 22 (offset: 21))
            |"SHOW AUTH RULES YIELD"
            |                      ^""".stripMargin
        ).withSyntaxErrorGqlStatus(
          gqlStatus(
            GqlStatusInfoCodes.STATUS_42I06,
            "error: syntax error or access rule violation - invalid input. Invalid input '', expected: a variable name, '*', ',', 'PRIVILEGE' or 'PRIVILEGES'."
          )
        )
    }
  }
}
