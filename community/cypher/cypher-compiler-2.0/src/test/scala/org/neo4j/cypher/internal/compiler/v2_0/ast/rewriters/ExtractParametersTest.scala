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
class ExtractParametersTest extends FunSuite {
  import parser.ParserFixture._

  test("should extract literals in return clause") {
    List(1,1.1,true,false,null).foreach{ v => assertRewrite(s"RETURN $v as result", s"RETURN {`P $v`} as result") }
    assertRewrite("RETURN 'apa' as result", "RETURN {`P apa`} as result")
    assertRewrite("RETURN \"apa\" as result", "RETURN {`P apa`} as result")
  }
  test("should extract literals in match clause") {
    List(1,1.1,true,false,null).foreach{ v => assertRewrite(s"MATCH ({a:$v})", s"MATCH ({a:{`P $v`}})") }
    assertRewrite("MATCH ({a:'apa'})", "MATCH ({a:{`P apa`}})")
    assertRewrite("MATCH ({a:\"apa\"})", "MATCH ({a:{`P apa`}})")
  }
  test("should extract literals in skip limit clause") {
    assertRewrite(s"RETURN 0 as x SKIP 1 limit 2", s"RETURN {`P 0`} as x SKIP {`P 1`} LIMIT {`P 2`}")
  }
  test("should extract literals in merge clause") {
    List(1,1.1,true,false,null).foreach{ v => assertRewrite(s"MERGE (n {a:$v}) ON CREATE SET n.foo = $v ON MATCH SET n.foo = $v", s"MERGE (n {a:{`P $v`}}) ON CREATE SET n.foo = {`P $v`} ON MATCH SET n.foo = {`P $v`}") }
    assertRewrite(s"MERGE (n {a:'apa'}) ON CREATE SET n.foo = 'apa' ON MATCH SET n.foo = 'apa'", s"MERGE (n {a:{`P apa`}}) ON CREATE SET n.foo = {`P apa`} ON MATCH SET n.foo = {`P apa`}")
//    assertRewrite(s"MERGE (n {a:\"apa\"}) ON CREATE SET n.foo = \"apa\" ON MATCH SET n.foo = \"apa\"", s"MERGE (n {a:{`P apa`}}) ON CREATE SET n.foo = {`P apa`} ON MATCH SET n.foo = {`P apa`}")
  }

  private def assertRewrite(originalQuery: String, expectedQuery: String) {
    val original = parser.parse(originalQuery)
    val expected = parser.parse(expectedQuery)

    val replacements = original.fold(Map.empty[(Any,InputPosition), ast.Parameter]) {
      case l: ast.Literal => acc => acc + ((l.value,l.position) -> ast.Parameter(s"P ${l.value}")(l.position))
    }

    val result = original.rewrite(bottomUp(ExtractParameters(replacements)))
    assert(result === expected)
  }
}
