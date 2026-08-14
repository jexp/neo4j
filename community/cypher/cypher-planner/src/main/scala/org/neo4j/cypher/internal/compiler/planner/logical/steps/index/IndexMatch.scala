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
package org.neo4j.cypher.internal.compiler.planner.logical.steps.index

import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor

/**
 * Represents a possible use of an index which can be used to solve a given sequence of predicates.
 *
 * This trait is to provide a unified interface of the two index match types for node indexes and relationship indexes.
 */
trait IndexMatch {
  def propertyPredicates: Seq[IndexCompatiblePredicate]
  def indexDescriptor: IndexDescriptor

  def predicateSet(
    newPredicates: Seq[IndexCompatiblePredicate],
    exactPredicatesCanGetValue: Boolean,
    context: LogicalPlanningContext,
    queryGraph: QueryGraph
  ): PredicateSet
  def variable: LogicalVariable
}
