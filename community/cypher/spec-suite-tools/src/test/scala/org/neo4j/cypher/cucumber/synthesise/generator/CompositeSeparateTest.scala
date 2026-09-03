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

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

/**
 * Exercises the pure, parser-parameterised logic on the `CompositeSeparate` companion object -- the same shape of
 * test [[CompositeStitchedTest]] runs against `CompositeUse.route`. The instance-level `filter`/`generateScenarios`
 * (which additionally need a full `CucumberSalad.Ingredients`) are thin wiring over these predicates and over
 * [[CompositeUse.route]] (already covered by [[CompositeStitchedTest]]), so they are not re-instantiated here.
 */
class CompositeSeparateTest extends CypherFunSuite {

  private val parser = new CachingParser(CypherVersion.Cypher25)

  private def assertWraps(query: String, expected: String): Unit = {
    val result = CompositeSeparate.wrap(parser, query)
    withClue(s"wrapped query:\n$result\n") {
      assert(result == expected)
      try parser.parse(result)
      catch { case e: Exception => fail(s"wrapped query did not parse:\n$result", e) }
    }
  }

  test("a plain query with multiple aliased returns is wrapped and re-projected in declared order") {
    assertWraps(
      "MATCH (n) RETURN n.p AS x, n.q AS y",
      """CALL () {
        |USE comp.data
        |MATCH (n) RETURN n.p AS x, n.q AS y
        |}
        |RETURN `x`, `y`""".stripMargin
    )
  }

  test("a top-level UNION is wrapped as a whole, re-projected using the first branch's columns") {
    assertWraps(
      "RETURN 1 AS a UNION RETURN 2 AS a",
      """CALL () {
        |USE comp.data
        |RETURN 1 AS a UNION USE comp.data
        |RETURN 2 AS a
        |}
        |RETURN `a`""".stripMargin
    )
  }

  test("a NEXT query with nested UNION/WHEN is wrapped whole in USE-braces and re-projected with RETURN *") {
    assertWraps(
      """      UNWIND [1, 2] AS x
        |      RETURN x
        |
        |      NEXT
        |
        |      {
        |        WITH *, 3 AS b, 4 AS z
        |        RETURN *
        |        UNION
        |        WITH *, 3 AS b
        |        CALL(*) {
        |          WHEN b > 3 THEN {
        |            WITH *, 3 AS c
        |            RETURN x + c AS z
        |          }
        |          ELSE {
        |            RETURN x + 3 AS z
        |          }
        |        }
        |        RETURN *
        |      }""".stripMargin,
      """CALL () {
        |USE comp.data {
        |      UNWIND [1, 2] AS x
        |      RETURN x
        |
        |      NEXT
        |
        |      {
        |        WITH *, 3 AS b, 4 AS z
        |        RETURN *
        |        UNION
        |        WITH *, 3 AS b
        |        CALL(*) {
        |          WHEN b > 3 THEN {
        |            WITH *, 3 AS c
        |            RETURN x + c AS z
        |          }
        |          ELSE {
        |            RETURN x + 3 AS z
        |          }
        |        }
        |        RETURN *
        |      }
        |}
        |}
        |RETURN *""".stripMargin
    )
  }

  test("WHEN is wrapped as a single USE { ... } and re-projected using the first branch's columns") {
    assertWraps(
      "WHEN true THEN RETURN 1 AS x ELSE RETURN 2 AS x",
      """CALL () {
        |USE comp.data {
        |WHEN true THEN RETURN 1 AS x ELSE RETURN 2 AS x
        |}
        |}
        |RETURN `x`""".stripMargin
    )
  }

  test("a non-returning write is wrapped as an updating subquery with no outer projection") {
    assertWraps(
      "CREATE (n)",
      "CALL () {\nUSE comp.data\nCREATE (n)\n}\n"
    )
  }

  test("a RETURN * query keeps an outer RETURN *") {
    assertWraps(
      "MATCH (n) RETURN *",
      """CALL () {
        |USE comp.data
        |MATCH (n) RETURN *
        |}
        |RETURN *""".stripMargin
    )
  }

  test("the outer projection carries no USE (the subquery boundary keeps the selection contained)") {
    val result = CompositeSeparate.wrap(parser, "MATCH (n) RETURN n.p AS x")
    val outer = result.substring(result.lastIndexOf("}") + 1)
    assert(!outer.contains("USE"))
  }

  test("isSingleGraphWrappable rejects a query that already selects a graph") {
    assert(!CompositeUse.isSingleGraphWrappable(parser.parse("USE neo4j MATCH (n) RETURN n.p AS x")))
  }

  test("isSingleGraphWrappable accepts a query with no graph selection") {
    assert(CompositeUse.isSingleGraphWrappable(parser.parse("MATCH (n) RETURN n.p AS x")))
  }

  test("a terminal ORDER BY/SKIP/LIMIT stays inside the subquery and is dropped from the outer projection") {
    val result = CompositeSeparate.wrap(parser, "MATCH (n) RETURN n.p AS x ORDER BY x SKIP 1 LIMIT 2")
    withClue(s"wrapped query:\n$result\n") {
      assert(result.contains("ORDER BY"))
      val outer = result.substring(result.lastIndexOf("}") + 1).trim
      assert(outer == "RETURN `x`")
      try parser.parse(result)
      catch { case e: Exception => fail(s"wrapped query did not parse:\n$result", e) }
    }
  }
}
