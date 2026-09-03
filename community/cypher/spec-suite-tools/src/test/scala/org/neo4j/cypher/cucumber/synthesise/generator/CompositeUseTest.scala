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

class CompositeUseTest extends CypherFunSuite {

  private val parser = new CachingParser(CypherVersion.Cypher25)

  test("isSingleGraphWrappable rejects a query that already selects a graph") {
    assert(!CompositeUse.isSingleGraphWrappable(parser.parse("USE neo4j MATCH (n) RETURN n")))
  }

  test("isSingleGraphWrappable accepts a query with no graph selection") {
    assert(CompositeUse.isSingleGraphWrappable(parser.parse("MATCH (n) RETURN n")))
  }

  test("routeWrapsInBraces is true for a WHEN") {
    assert(CompositeUse.routeWrapsInBraces(parser.parse("WHEN true THEN RETURN 1 AS x ELSE RETURN 2 AS x")))
  }

  test("routeWrapsInBraces is false for a plain query") {
    assert(!CompositeUse.routeWrapsInBraces(parser.parse("MATCH (n) RETURN n AS x")))
  }

  test("routeWrapsInBraces is false for a top-level UNION") {
    assert(!CompositeUse.routeWrapsInBraces(parser.parse("RETURN 1 AS a UNION RETURN 2 AS a")))
  }

  test("wrapStitched leaf-splices USE at the top level, with no enclosing CALL") {
    val result = CompositeUse.wrapStitched(parser, "MATCH (n) RETURN n.p AS x")
    assert(result == "USE comp.data\nMATCH (n) RETURN n.p AS x")
  }

  test("wrapStitched strips a trailing semicolon") {
    val result = CompositeUse.wrapStitched(parser, "MATCH (n) RETURN n AS p;")
    assert(result.startsWith("USE comp.data"))
    assert(!result.contains("CALL ("))
    assert(!result.contains(";"))
  }
}
