/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.v2_0.ast.rewriters

import org.neo4j.cypher.internal.compiler.v2_0._
import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class LiteralReplacementTest extends FunSuite {
  import parser.ParserFixture._

  test("should extract literals in return clause") {
    List(1,1.1,true,false,null).foreach { v =>
      assertRewrite(s"RETURN $v as result", s"RETURN {`  AUTO7`} as result", Map("  AUTO7" -> v))
    }
    assertRewrite("RETURN 'apa' as result", "RETURN {`  AUTO7`} as result", Map("  AUTO7" -> "apa"))
    assertRewrite("RETURN \"apa\" as result", "RETURN {`  AUTO7`} as result", Map("  AUTO7" -> "apa"))
  }

  test("should extract literals in match clause") {
    List(1,1.1,true,false,null).foreach { v =>
      assertRewrite(s"MATCH ({a:$v})", s"MATCH ({a:{`  AUTO10`}})", Map("  AUTO10" -> v))
    }
    assertRewrite("MATCH ({a:'apa'})", "MATCH ({a:{`  AUTO10`}})", Map("  AUTO10" -> "apa"))
    assertRewrite("MATCH ({a:\"apa\"})", "MATCH ({a:{`  AUTO10`}})", Map("  AUTO10" -> "apa"))
  }

  test("should extract literals in skip limit clause") {
    assertRewrite(
      s"RETURN 0 as x SKIP 1 limit 2",
      s"RETURN {`  AUTO7`} as x SKIP {`  AUTO19`} LIMIT {`  AUTO27`}",
      Map("  AUTO7" -> 0, "  AUTO19" -> 1, "  AUTO27" -> 2)
    )
  }

  test("should extract literals in merge clause") {
    List(1,1.1,true,false).foreach { v =>
      val vs = v.toString.padTo(6, ' ')
      assertRewrite(
        s"MERGE (n {a:$vs}) ON CREATE SET n.foo = $vs ON MATCH SET n.foo = $vs",
        s"MERGE (n {a:{`  AUTO12`}}) ON CREATE SET n.foo = {`  AUTO43`} ON MATCH SET n.foo = {`  AUTO71`}",
        Map("  AUTO12" -> v, "  AUTO43" -> v, "  AUTO71" -> v)
      )
    }
    assertRewrite(
      s"MERGE (n {a:null}) ON CREATE SET n.foo = null ON MATCH SET n.foo = null",
      s"MERGE (n {a:{`  AUTO12`}}) ON CREATE SET n.foo = {`  AUTO41`} ON MATCH SET n.foo = {`  AUTO67`}",
      Map("  AUTO12" -> null, "  AUTO41" -> null, "  AUTO67" -> null)
    )
    assertRewrite(
      s"MERGE (n {a:'apa'}) ON CREATE SET n.foo = 'apa' ON MATCH SET n.foo = 'apa'",
      s"MERGE (n {a:{`  AUTO12`}}) ON CREATE SET n.foo = {`  AUTO42`} ON MATCH SET n.foo = {`  AUTO69`}",
      Map("  AUTO12" -> "apa", "  AUTO42" -> "apa", "  AUTO69" -> "apa")
    )
  }

  private def assertRewrite(originalQuery: String, expectedQuery: String, replacements: Map[String, Any]) {
    val original = parser.parse(originalQuery)
    val expected = parser.parse(expectedQuery)

    val (rewriter, _) = literalReplacement(original)

    val result = original.rewrite(bottomUpExpressions(rewriter))
    assert(result === expected)
  }
}
