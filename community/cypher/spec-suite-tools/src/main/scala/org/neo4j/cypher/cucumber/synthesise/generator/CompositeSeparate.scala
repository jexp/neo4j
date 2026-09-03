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

import org.neo4j.cypher.cucumber.synthesise.CucumberSalad
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsParameters

/**
 * The routed query is placed inside an empty-scope subquery `CALL () { <routed Q> }`, so the outer stays on the
 * composite `comp` while the inner selects the constituent — the fabric stitcher fragments rather than stitches. A
 * terminal ORDER BY/SKIP/LIMIT stays inside the subquery; the outer projection re-selects the returned columns by
 * name. Compare [[CompositeStitched]].
 */
class CompositeSeparate(args: CucumberSalad.Ingredients) extends CompositeGenerator(args) {
  override val name: String = "composite-separate"
  override protected def namePrefix: String = "Composite-separate"

  override protected def wrapDescription: String =
    s"composite-separate-wrapping (CALL () { USE $constituent ... })"
  override protected def testWrap(cypher: String): String = CompositeSeparate.wrap(args.parser, cypher, usePrefix)

  override def filter: Filter = separateFilter

  // A `CALL {}` body can't hold a standalone call or an updating-in-transactions clause, and its returns must be legal.
  protected def separateFilter: Filter = compositeFilter
    .testQueries(_.forall(q =>
      Filter.returnsAreSubqueryLegal(q) &&
        Filter.isNotStandaloneCall(q) &&
        Filter.doNotContainAst[InTransactionsParameters](q)
    ))
}

object CompositeSeparate {

  def wrap(parser: CachingParser, cypher: String, usePrefix: String = CompositeUse.usePrefix): String =
    CompositeUse.peelDirectives(cypher)(separateWrap(parser, usePrefix, _))

  private def separateWrap(parser: CachingParser, usePrefix: String, cypher: String): String = {
    val inner = CompositeUse.route(parser, usePrefix, cypher)
    val outer = CompositeUse.outerReturn(parser.parse(cypher).ast, keepStar = true).getOrElse("")
    s"CALL () {\n$inner\n}\n$outer"
  }
}
