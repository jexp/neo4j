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

class FilterTest extends CypherFunSuite {

  private val parser = new CachingParser(CypherVersion.Cypher25)

  test("returnsAreSubqueryLegal rejects an unaliased expression return item") {
    assert(!Filter.returnsAreSubqueryLegal(parser.parse("MATCH (n) RETURN n.p")))
  }

  test("returnsAreSubqueryLegal accepts an aliased expression return item") {
    assert(Filter.returnsAreSubqueryLegal(parser.parse("MATCH (n) RETURN n.p AS x")))
  }

  test("returnsAreSubqueryLegal accepts a bare variable return item") {
    assert(Filter.returnsAreSubqueryLegal(parser.parse("MATCH (n) RETURN n")))
  }

  test("returnsAreSubqueryLegal accepts a star return item") {
    assert(Filter.returnsAreSubqueryLegal(parser.parse("MATCH (n) RETURN *")))
  }

  test("isNotStandaloneCall rejects a standalone procedure call without parens") {
    assert(!Filter.isNotStandaloneCall(parser.parse("CALL db.labels")))
  }

  test("isNotStandaloneCall rejects a standalone procedure call") {
    assert(!Filter.isNotStandaloneCall(parser.parse("CALL db.labels()")))
  }

  test("isNotStandaloneCall accepts a procedure call with YIELD") {
    assert(Filter.isNotStandaloneCall(parser.parse("CALL db.labels() YIELD label RETURN label")))
  }

  test("isNotStandaloneCall accepts a plain query") {
    assert(Filter.isNotStandaloneCall(parser.parse("MATCH (n) RETURN n")))
  }

  test("dropTrailingSemicolon strips a trailing semicolon") {
    Filter.dropTrailingSemicolon("MATCH (n) RETURN n AS p;") shouldBe "MATCH (n) RETURN n AS p"
  }

  test("dropTrailingSemicolon strips a trailing semicolon plus trailing whitespace") {
    Filter.dropTrailingSemicolon("RETURN 1 AS x;  ") shouldBe "RETURN 1 AS x"
  }

  test("dropTrailingSemicolon leaves a query with no trailing semicolon unchanged") {
    Filter.dropTrailingSemicolon("RETURN 'a;' AS x") shouldBe "RETURN 'a;' AS x"
  }
}
