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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.InterpretedRuntimeTestSuite
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.ValueComparisonHelper.beEquivalentTo
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.storable.Values.intValue

class OptionalPipeTest extends InterpretedRuntimeTestSuite with PipeTestSupport {

  test("should null the nullable variables of a copy of the incoming row when the source is empty") {
    val incoming = CypherRow.from("a" -> intValue(1), "b" -> intValue(2))
    val state = QueryStateHelper.emptyWith(initialContext = Some(incoming))

    val result = OptionalPipe(Set("b", "c"), new FakePipe(Iterator.empty))().createResults(state).toList

    result should beEquivalentTo(List(Map("a" -> 1, "b" -> NO_VALUE, "c" -> NO_VALUE)))
    (result.head eq incoming) shouldBe false
    List(incoming) should beEquivalentTo(List(Map("a" -> 1, "b" -> 2)))
  }

  test("should fabricate a row when the source is empty and there is no incoming row") {
    val result =
      OptionalPipe(Set("a", "b"), new FakePipe(Iterator.empty))().createResults(QueryStateHelper.empty).toList

    result should beEquivalentTo(List(Map("a" -> NO_VALUE, "b" -> NO_VALUE)))
  }

  test("should pass on the source rows and leave the incoming row alone when the source is not empty") {
    val incoming = CypherRow.from("a" -> intValue(1), "b" -> intValue(2))
    val state = QueryStateHelper.emptyWith(initialContext = Some(incoming))
    val sourceData = List(Map("a" -> 1, "b" -> 3), Map("a" -> 1, "b" -> 4))

    val result = OptionalPipe(Set("b"), new FakePipe(sourceData.iterator))().createResults(state).toList

    result should beEquivalentTo(sourceData)
    List(incoming) should beEquivalentTo(List(Map("a" -> 1, "b" -> 2)))
  }
}
