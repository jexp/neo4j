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

import org.neo4j.cypher.cucumber.glue.regular.CompositeExecutorPool
import org.neo4j.cypher.cucumber.synthesise.CucumberSalad
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.TransactionHandling

/**
 * Like [[CompositeStitched]] but targets a *local* constituent, so the query executes on the fabric leader (asLocal)
 * and passes through fabricFinalize — the leader-side path the self-remote [[CompositeStitched]] bypasses (asRemote).
 */
class CompositeLocal(args: CucumberSalad.Ingredients) extends CompositeStitched(args) {
  override val name: String = "composite-local"
  override protected def namePrefix: String = "Composite-local"
  override protected def constituent: String = CompositeExecutorPool.LocalConstituent

  // The open-tx side-effect recorder routes via the self-remote constituent (RegularCypherSteps.graphStateRoutePrefix),
  // a different fabric child tx than our local one, so it can't observe our writes within the open transaction.
  override def filter: Filter = super.filter.steps[TransactionHandling](_.isEmpty)
}
