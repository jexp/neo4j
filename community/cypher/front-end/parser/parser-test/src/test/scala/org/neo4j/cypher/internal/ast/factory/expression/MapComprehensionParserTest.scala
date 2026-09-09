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
package org.neo4j.cypher.internal.ast.factory.expression

import org.neo4j.cypher.internal.ast.IsNotTyped
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5
import org.neo4j.cypher.internal.ast.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.IsNotNull
import org.neo4j.cypher.internal.expressions.MapComprehension
import org.neo4j.cypher.internal.expressions.MapEntriesComprehension
import org.neo4j.cypher.internal.expressions.Multiply
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.util.symbols.ClosedDynamicUnionType
import org.neo4j.cypher.internal.util.symbols.IntegerType
import org.neo4j.cypher.internal.util.symbols.NullType

class MapComprehensionParserTest extends AstParsingTestBase {

  test("{ a in p WHERE a.foo > 123 | a.name : a.foo }") {
    "{ a in p WHERE a.foo > 123 | a.name : a.foo }" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst(
          MapComprehension(
            varFor("a"),
            varFor("p"),
            Some(GreaterThan(
              Property(varFor("a"), propName("foo"))(pos),
              SignedDecimalIntegerLiteral("123")(pos)
            )(pos)),
            Property(varFor("a"), propName("name"))(pos),
            Property(varFor("a"), propName("foo"))(pos)
          )(pos)
        )
    }
  }

  test("{ a in p | a.name : a.foo }") {
    "{ a in p | a.name : a.foo }" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst(
          MapComprehension(
            varFor("a"),
            varFor("p"),
            None,
            Property(varFor("a"), propName("name"))(pos),
            Property(varFor("a"), propName("foo"))(pos)
          )(pos)
        )
    }
  }

  test("{ a in p WHERE a.foo > 123 | 'key' : a.foo }") {
    "{ a in p WHERE a.foo > 123 | 'key' : a.foo }" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst(
          MapComprehension(
            varFor("a"),
            varFor("p"),
            Some(GreaterThan(
              Property(varFor("a"), propName("foo"))(pos),
              SignedDecimalIntegerLiteral("123")(pos)
            )(pos)),
            literalString("key"),
            Property(varFor("a"), propName("foo"))(pos)
          )(pos)
        )
    }
  }

  test("{a IN (true IS NOT :: INTEGER) | NULL : NULL }") {
    "{a IN (true IS NOT :: INTEGER) | NULL : NULL }" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst(
          MapComprehension(
            varFor("a"),
            IsNotTyped(
              trueLiteral,
              IntegerType(isNullable = true)(pos)
            )(pos),
            None,
            nullLiteral,
            nullLiteral
          )(pos)
        )
    }
  }

  test("{a IN (true IS NOT :: INTEGER | NULL) | c : d}") {
    "{a IN (true IS NOT :: INTEGER | NULL) | c : d}" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst(
          MapComprehension(
            varFor("a"),
            IsNotTyped(
              trueLiteral,
              ClosedDynamicUnionType(
                Set(
                  IntegerType(isNullable = true)(pos),
                  NullType()(pos)
                )
              )(pos)
            )(pos),
            None,
            varFor("c"),
            varFor("d")
          )(pos)
        )
    }
  }

  test("simple map comprehension") {
    "{ k IN keys(map) | k : map[k] * 10 }" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst(
          MapComprehension(
            varFor("k"),
            function("keys", varFor("map")),
            None,
            varFor("k"),
            Multiply(containerIndex(varFor("map"), varFor("k")), literalInt(10))(pos)
          )(pos)
        )
    }
  }

  test("cleaning an existing map with a WHERE predicate, list version") {
    "{ k IN keys(map) WHERE map[k] IS NOT NULL | k : map[k] }" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst(
          MapComprehension(
            varFor("k"),
            function("keys", varFor("map")),
            Some(IsNotNull(containerIndex(varFor("map"), varFor("k")))(pos)),
            varFor("k"),
            containerIndex(varFor("map"), varFor("k"))
          )(pos)
        )
    }
  }

  test("a constant key expression parses, even though only the last written entry will survive semantically") {
    "{ k IN keys(map) | 'a' : map[k] * 10 }" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst(
          MapComprehension(
            varFor("k"),
            function("keys", varFor("map")),
            None,
            literalString("a"),
            Multiply(containerIndex(varFor("map"), varFor("k")), literalInt(10))(pos)
          )(pos)
        )
    }
  }

  test("nested map comprehensions") {
    """{
      |  k IN keys(nestedMap)
      |  | k : { innerK IN keys(nestedMap[k]) | innerK : nestedMap[k][innerK] * 10 }
      |}""".stripMargin should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst(
          MapComprehension(
            varFor("k"),
            function("keys", varFor("nestedMap")),
            None,
            varFor("k"),
            MapComprehension(
              varFor("innerK"),
              function("keys", containerIndex(varFor("nestedMap"), varFor("k"))),
              None,
              varFor("innerK"),
              Multiply(
                containerIndex(containerIndex(varFor("nestedMap"), varFor("k")), varFor("innerK")),
                literalInt(10)
              )(pos)
            )(pos)
          )(pos)
        )
    }
  }

  test("a null source list parses, even though it will evaluate to null semantically") {
    "{ i IN null | 'a' : i }" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst(
          MapComprehension(
            varFor("i"),
            nullLiteral,
            None,
            literalString("a"),
            varFor("i")
          )(pos)
        )
    }
  }

  test("a null predicate parses, even though it will evaluate to an empty map semantically") {
    "{ i IN range(0, 10) WHERE null | 'a': i }" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst(
          MapComprehension(
            varFor("i"),
            function("range", literalInt(0), literalInt(10)),
            Some(nullLiteral),
            literalString("a"),
            varFor("i")
          )(pos)
        )
    }
  }

  test("a non-boolean predicate parses, even though it is a type mismatch semantically") {
    "{ i IN range(0, 10) WHERE 1 | 'a': i }" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst(
          MapComprehension(
            varFor("i"),
            function("range", literalInt(0), literalInt(10)),
            Some(literalInt(1)),
            literalString("a"),
            varFor("i")
          )(pos)
        )
    }
  }

  test("a null key expression parses, even though the key must not be null semantically") {
    "{ i IN range(0, 10) | null : i }" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst(
          MapComprehension(
            varFor("i"),
            function("range", literalInt(0), literalInt(10)),
            None,
            nullLiteral,
            varFor("i")
          )(pos)
        )
    }
  }

  test("a non-string key expression parses, even though the key must be a string semantically") {
    "{ i IN range(0, 10) | i : i }" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst(
          MapComprehension(
            varFor("i"),
            function("range", literalInt(0), literalInt(10)),
            None,
            varFor("i"),
            varFor("i")
          )(pos)
        )
    }
  }

  test("a key expression that binds looser than expression6 requires parentheses, list version") {
    "{ i IN range(0, 10) | (i = i) : i }" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst(
          MapComprehension(
            varFor("i"),
            function("range", literalInt(0), literalInt(10)),
            None,
            Equals(varFor("i"), varFor("i"))(pos),
            varFor("i")
          )(pos)
        )
    }
  }

  test("a null value expression, list version") {
    "{ i IN range(0, 2) | toString(i) : null }" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst(
          MapComprehension(
            varFor("i"),
            function("range", literalInt(0), literalInt(2)),
            None,
            function("toString", varFor("i")),
            nullLiteral
          )(pos)
        )
    }
  }

  test("missing bar and key/value pair fails to parse") {
    "{ i IN range(0,10) }" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _       => _.withSyntaxErrorContaining("expected an expression, 'WHERE' or '|'")
    }
  }

  test("missing colon and value expression fails to parse") {
    "{ i IN range(0,10) | toString(i) }" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _       => _.withSyntaxErrorContaining("expected an expression or ':'")
    }
  }

  test("variable introduced inside a map comprehension is not available outside it") {
    "WITH { i IN range(0,10) | toString(i) : i } AS map RETURN i" should parseIn[Statement] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst(
          singleQuery(
            with_(
              aliasedReturnItem(
                MapComprehension(
                  varFor("i"),
                  function("range", literalInt(0), literalInt(10)),
                  None,
                  function("toString", varFor("i")),
                  varFor("i")
                )(pos),
                "map"
              )
            ),
            return_(returnItem(varFor("i"), "i"))
          )
        )
    }
  }

  test("the comprehension's variable shadows an outer variable of the same name") {
    "WITH 'a' AS i RETURN { i IN range(0,2) | toString(i) : i }, i" should parseIn[Statement] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst(
          singleQuery(
            with_(aliasedReturnItem(literalString("a"), "i")),
            return_(
              returnItem(
                MapComprehension(
                  varFor("i"),
                  function("range", literalInt(0), literalInt(2)),
                  None,
                  function("toString", varFor("i")),
                  varFor("i")
                )(pos),
                "{ i IN range(0,2) | toString(i) : i }"
              ),
              returnItem(varFor("i"), "i")
            )
          )
        )
    }
  }

  test("{ k: v IN map WHERE v > 123 | k : v }") {
    "{ k: v IN map WHERE v > 123 | k : v }" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst(
          MapEntriesComprehension(
            varFor("k"),
            varFor("v"),
            varFor("map"),
            Some(GreaterThan(varFor("v"), SignedDecimalIntegerLiteral("123")(pos))(pos)),
            varFor("k"),
            varFor("v")
          )(pos)
        )
    }
  }

  test("{ k: v IN map | k : v }") {
    "{ k: v IN map | k : v }" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst(
          MapEntriesComprehension(
            varFor("k"),
            varFor("v"),
            varFor("map"),
            None,
            varFor("k"),
            varFor("v")
          )(pos)
        )
    }
  }

  test("{ k: v IN map WHERE v > 123 | 'key' : v }") {
    "{ k: v IN map WHERE v > 123 | 'key' : v }" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst(
          MapEntriesComprehension(
            varFor("k"),
            varFor("v"),
            varFor("map"),
            Some(GreaterThan(varFor("v"), SignedDecimalIntegerLiteral("123")(pos))(pos)),
            literalString("key"),
            varFor("v")
          )(pos)
        )
    }
  }

  test("simple map entries comprehension") {
    "{ k: v IN map | k : v * 10 }" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst(
          MapEntriesComprehension(
            varFor("k"),
            varFor("v"),
            varFor("map"),
            None,
            varFor("k"),
            multiply(varFor("v"), literalInt(10))
          )(pos)
        )
    }
  }

  test("cleaning an existing map with a WHERE predicate") {
    "{ k: v IN map WHERE v IS NOT NULL | k : v }" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst(
          MapEntriesComprehension(
            varFor("k"),
            varFor("v"),
            varFor("map"),
            Some(isNotNull(varFor("v"))),
            varFor("k"),
            varFor("v")
          )(pos)
        )
    }
  }

  test("nested map entries comprehensions") {
    """{
      |  k: v IN outerMap
      |  | k : { innerK: innerV IN v | innerK : innerV * 10 }
      |}""".stripMargin should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst(
          MapEntriesComprehension(
            varFor("k"),
            varFor("v"),
            varFor("outerMap"),
            None,
            varFor("k"),
            MapEntriesComprehension(
              varFor("innerK"),
              varFor("innerV"),
              varFor("v"),
              None,
              varFor("innerK"),
              multiply(varFor("innerV"), literalInt(10))
            )(pos)
          )(pos)
        )
    }
  }

  test("a null source map parses, even though it will evaluate to null semantically") {
    "{ k: v IN null | 'a' : v }" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst(
          MapEntriesComprehension(
            varFor("k"),
            varFor("v"),
            nullLiteral,
            None,
            literalString("a"),
            varFor("v")
          )(pos)
        )
    }
  }

  test("a non-map source parses, even though it is a type mismatch semantically") {
    "{ k: v IN [1, 2, 3] | k : v }" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst(
          MapEntriesComprehension(
            varFor("k"),
            varFor("v"),
            listOf(literalInt(1), literalInt(2), literalInt(3)),
            None,
            varFor("k"),
            varFor("v")
          )(pos)
        )
    }
  }

  test("a key expression that binds looser than expression6 requires parentheses") {
    "{ k: v IN map | k = k : v }" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _       => _.withSyntaxErrorContaining("Invalid input '=': expected an expression or ':'")
    }
  }

  test("a null value expression") {
    "{ k: v IN map | k : null }" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst(
          MapEntriesComprehension(
            varFor("k"),
            varFor("v"),
            varFor("map"),
            None,
            varFor("k"),
            nullLiteral
          )(pos)
        )
    }
  }

  test("{ k: v IN map } parses as a map literal, since the colon reads as an ordinary map entry without a bar") {
    "{ k: v IN map }" should parseIn[Expression] {
      case _ => _.toAst(mapOf("k" -> in(varFor("v"), varFor("map"))))
    }
  }

  test("missing colon and value expression fails to parse (map entries)") {
    "{ k: v IN map | k }" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _       => _.withSyntaxErrorContaining("expected an expression or ':'")
    }
  }

  test("variables introduced inside a map entries comprehension are not available outside it") {
    "WITH { k: v IN map | k : v } AS result RETURN k, v" should parseIn[Statement] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst(
          singleQuery(
            with_(
              aliasedReturnItem(
                MapEntriesComprehension(
                  varFor("k"),
                  varFor("v"),
                  varFor("map"),
                  None,
                  varFor("k"),
                  varFor("v")
                )(pos),
                "result"
              )
            ),
            return_(returnItem(varFor("k"), "k"), returnItem(varFor("v"), "v"))
          )
        )
    }
  }

  test("the key and value variables shadow outer variables of the same name") {
    "WITH 'a' AS k, 'b' AS v RETURN { k: v IN map | k : v }, k, v" should parseIn[Statement] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst(
          singleQuery(
            with_(aliasedReturnItem(literalString("a"), "k"), aliasedReturnItem(literalString("b"), "v")),
            return_(
              returnItem(
                MapEntriesComprehension(
                  varFor("k"),
                  varFor("v"),
                  varFor("map"),
                  None,
                  varFor("k"),
                  varFor("v")
                )(pos),
                "{ k: v IN map | k : v }"
              ),
              returnItem(varFor("k"), "k"),
              returnItem(varFor("v"), "v")
            )
          )
        )
    }
  }
}
