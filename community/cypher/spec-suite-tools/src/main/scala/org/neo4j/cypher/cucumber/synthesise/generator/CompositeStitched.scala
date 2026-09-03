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
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.QueryExecution

/**
 * `USE <constituent>` is routed to each top-level leaf (see [[CompositeUse.route]]), so the fabric stitcher collapses
 * the whole query into one remote fragment. Compare [[CompositeSeparate]].
 */
class CompositeStitched(args: CucumberSalad.Ingredients) extends CompositeGenerator(args) {
  override val name: String = "composite-stitched"
  override protected def namePrefix: String = "Composite"
  override protected def wrapDescription: String = s"composite-wrapping (USE $constituent)"
  override protected def testWrap(cypher: String): String = stitchedWrap(cypher)

  // A brace-wrapped shape (WHEN/NEXT/local-defs/top-level-braces) becomes a subquery, so its returns must be legal there.
  override def filter: Filter = compositeFilter
    .queries[QueryExecution](_.forall(q => !CompositeUse.routeWrapsInBraces(q) || Filter.returnsAreSubqueryLegal(q)))
}
