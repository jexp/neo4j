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
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.InterpretedRuntimeTestSuite
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.LiteralHelper.literal
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ToStringFunctionCypher25
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ToStringFunctionCypher5
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ToStringOrNullFunctionCypher25
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ToStringOrNullFunctionCypher5
import org.neo4j.cypher.internal.util.test_helpers.CypherScalaCheckDrivenPropertyChecks
import org.neo4j.cypher.internal.util.test_helpers.GqlExceptionMatchers.functionArgumentGqlException
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.values.AnyValue
import org.neo4j.values.AnyValueWriter
import org.neo4j.values.Equality
import org.neo4j.values.ValueMapper
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.ValueRepresentation
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.storable.Values.doubleValue
import org.neo4j.values.storable.Values.int8Vector
import org.neo4j.values.storable.Values.stringArray
import org.neo4j.values.storable.Values.stringValue
import org.neo4j.values.virtual.MapValueBuilder
import org.neo4j.values.virtual.NodeValue
import org.neo4j.values.virtual.RelationshipValue
import org.neo4j.values.virtual.VirtualValues
import org.scalacheck.Gen

import java.time.LocalDate

class ToStringFunctionTest extends InterpretedRuntimeTestSuite with CypherScalaCheckDrivenPropertyChecks {

  val tests: Seq[(Any => AnyValue, String)] =
    Seq((toString, "toString"), (toStringOrNullCypher5, "toStringOrNull"))

  tests.foreach { case (toStringFn, name) =>
    test(s"$name: should return null if argument is null") {
      assert(toStringFn(null) === NO_VALUE)
    }

    test(s"$name: should not change a string") {
      toStringFn("10.599") should be(stringValue("10.599"))
    }

    test(s"$name:  can handle empty string as null") {
      toStringFn("") should be(stringValue(""))
    }

    test(s"$name: should convert an integer to a string") {
      toStringFn(21) should be(stringValue("21"))
    }

    test(s"$name: should convert a float to a string") {
      toStringFn(23.34) should be(stringValue("23.34"))
    }

    test(s"$name: should convert a negative float to a string") {
      toStringFn(-12.66) should be(stringValue("-12.66"))
    }

    test(s"$name: should convert a negative integer to a string") {
      toStringFn(-12) should be(stringValue("-12"))
    }

    test(s"$name: should handle boolean false") {
      toStringFn(false) should be(stringValue("false"))
    }

    test(s"$name: should handle boolean true") {
      toStringFn(true) should be(stringValue("true"))
    }
  }

  // toString (Cypher 5)

  test("Cypher 5: should throw an exception if the argument is an object which cannot be converted to a string") {
    val caughtException = the[CypherTypeException] thrownBy toString(List(1, 24))
    caughtException should be(functionArgumentGqlException(
      "Invalid input for function 'toString()': Expected a String, UUID, Float, Integer, Boolean, Temporal, Duration or Vector, got: List{Int(1), Int(24)}",
      "toString()",
      "Expected the value [1, 24] to be of type STRING, UUID, FLOAT, INTEGER, BOOLEAN, TEMPORAL, DURATION or VECTOR, but was of type LIST<INTEGER NOT NULL> NOT NULL."
    ))
  }

  test("Cypher 5: should stringify a top-level vector") {
    toString(int8Vector(1, 2, 3)) should be(stringValue("vector([1, 2, 3], 3, INTEGER8)"))
  }

  // toString (Cypher 25)

  test("Cypher 25: should return null if argument is null") {
    toStringCypher25(null) should equal(NO_VALUE)
  }

  test("Cypher 25: should stringify an empty list") {
    toStringCypher25(List.empty) should be(stringValue("[]"))
  }

  test("Cypher 25: should stringify an empty map") {
    toStringCypher25(Map.empty) should be(stringValue("{}"))
  }

  test("Cypher 25: should stringify a list preserving order, applying toString to each element") {
    val nestedVectorList = VirtualValues.list(int8Vector(1, 2, 3))
    val input = List(1, "a", LocalDate.of(2027, 1, 1), true, nestedVectorList, null)

    toStringCypher25(input) should be(stringValue(
      "[1, a, 2027-01-01, true, [vector([1, 2, 3], 3, INTEGER8)], null]"
    ))
  }

  test("Cypher 25: should stringify a map with alphabetically ordered keys and backtick-quoted special keys") {
    val mapBuilder = new MapValueBuilder()
    mapBuilder.add("b", stringValue("b"))
    mapBuilder.add("a", stringValue("a"))
    mapBuilder.add("g special", doubleValue(1.2))

    toStringCypher25(mapBuilder.build()) should be(stringValue(
      "{a: a, b: b, `g special`: 1.2}"
    ))
  }

  test("Cypher 25: should backtick-quote an empty-string map key") {
    val mapBuilder = new MapValueBuilder()
    mapBuilder.add("", stringValue("a"))

    toStringCypher25(mapBuilder.build()) should be(stringValue("{``: a}"))
  }

  test("Cypher 25: should stringify a top-level vector") {
    toStringCypher25(int8Vector(1, 2, 3)) should be(stringValue("vector([1, 2, 3], 3, INTEGER8)"))
  }

  test("Cypher 25: should stringify nested lists and maps") {
    val mapBuilder = new MapValueBuilder()
    mapBuilder.add("x", VirtualValues.list(stringValue("y")))
    val nestedMap = mapBuilder.build()

    toStringCypher25(List(nestedMap)) should be(stringValue("[{x: [y]}]"))
  }

  test("Cypher 25: should stringify a node with no labels") {
    toStringCypher25(node(-10)) should be(stringValue("()"))
  }

  test("Cypher 25: should stringify a node with labels in alphabetical order") {
    toStringCypher25(node(-10, "B", "A")) should be(stringValue("(:A:B)"))
  }

  test("Cypher 25: should backtick-quote a label with special characters") {
    toStringCypher25(node(-10, "has space")) should be(stringValue("(:`has space`)"))
  }

  test("Cypher 25: should backtick-quote an empty-string label") {
    toStringCypher25(node(-10, "")) should be(stringValue("(:``)"))
  }

  test("Cypher 25: should stringify a relationship") {
    toStringCypher25(relationship(-20, node(-10), node(-11), "REL")) should be(stringValue("[:REL]"))
  }

  test("Cypher 25: should backtick-quote a relationship type with special characters") {
    toStringCypher25(relationship(-20, node(-10), node(-11), "has space")) should be(
      stringValue("[:`has space`]")
    )
  }

  test("Cypher 25: should backtick-quote an empty-string relationship type") {
    toStringCypher25(relationship(-20, node(-10), node(-11), "")) should be(stringValue("[:``]"))
  }

  test("Cypher 25: should stringify a path consisting of one node") {
    val n1 = node(-10, "A")

    toStringCypher25(VirtualValues.path(Array(n1), Array())) should be(
      stringValue("(:A)")
    )
  }

  test("Cypher 25: should stringify a path with a forward relationship") {
    val n1 = node(-10, "A")
    val n2 = node(-11, "B")
    val r = relationship(-20, n1, n2, "REL")

    toStringCypher25(VirtualValues.path(Array(n1, n2), Array(r))) should be(
      stringValue("(:A)-[:REL]->(:B)")
    )
  }

  test("Cypher 25: should stringify a path with a mix of forward and backward relationships") {
    val n1 = node(-10, "A")
    val n2 = node(-11, "B")
    val n3 = node(-12, "C")
    val r1 = relationship(-20, n1, n2, "FIRST")
    // r2 actually points from n3 to n2, i.e. backward relative to the path's traversal order
    val r2 = relationship(-21, n3, n2, "SECOND")

    toStringCypher25(VirtualValues.path(Array(n1, n2, n3), Array(r1, r2))) should be(
      stringValue("(:A)-[:FIRST]->(:B)<-[:SECOND]-(:C)")
    )
  }

  test("Cypher 25: should stringify a node nested in a list") {
    toStringCypher25(List(node(-10, "A"))) should be(stringValue("[(:A)]"))
  }

  test("Cypher 25: should throw an exception if the argument is an object which cannot be converted to a string") {
    // Every real Cypher value now stringifies (arrays/lists implement SequenceValue, so even the old
    // "unsupported" StringArray case is valid). UnsupportedTestValue matches none of the type checks in
    // CypherFunctions.toString, so it exercises the else-branch that would otherwise go untested.
    val caughtException = the[CypherTypeException] thrownBy toStringCypher25(UnsupportedTestValue)
    caughtException.getMessage should include(
      "Invalid input for function 'toString()': Expected a String, UUID, Float, Integer, Boolean, Temporal, Duration, Vector, List, Map, Node, Relationship or Path, got:"
    )
  }

  // toStringOrNull (Cypher 5)

  test("Cypher 5: toStringOrNull can handle map type as null") {
    toStringOrNullCypher5(Map("a" -> "b")) should equal(NO_VALUE)
  }

  test("Cypher 5: toStringOrNull can handle list types as null") {
    toStringOrNullCypher5(List("a", "b")) should equal(NO_VALUE)
  }

  test("Cypher 5: toStringOrNull can handle empty map as null") {
    toStringOrNullCypher5(Map.empty) should equal(NO_VALUE)
  }

  test("Cypher 5: toStringOrNull can handle empty list as null") {
    toStringOrNullCypher5(List.empty) should equal(NO_VALUE)
  }

  test("Cypher 5: toStringOrNull should stringify a top-level vector") {
    toStringOrNullCypher5(int8Vector(1, 2, 3)) should be(stringValue("vector([1, 2, 3], 3, INTEGER8)"))
  }

  test("Cypher 5: toStringOrNull should not throw an exception for any value") {
    val generator: Gen[Any] = Gen.oneOf[Any](Gen.numStr, Gen.alphaStr, Gen.posNum[Double], Gen.posNum[Int])

    forAll(generator) { s =>
      {
        toStringOrNullCypher5(s) should (be(a[TextValue]) or equal(NO_VALUE))
      }
    }
  }

  // toStringOrNull (Cypher 25)

  test("Cypher 25: toStringOrNull should return null if argument is null") {
    toStringOrNullCypher25(null) should equal(NO_VALUE)
  }

  test("Cypher 25: toStringOrNull should stringify a list instead of returning null") {
    toStringOrNullCypher25(List("a", "b")) should be(stringValue("[a, b]"))
  }

  test("Cypher 25: toStringOrNull should stringify a map instead of returning null") {
    toStringOrNullCypher25(Map("a" -> "b")) should be(stringValue("{a: b}"))
  }

  test("Cypher 25: toStringOrNull should stringify an empty list") {
    toStringOrNullCypher25(List.empty) should be(stringValue("[]"))
  }

  test("Cypher 25: toStringOrNull should stringify an empty map") {
    toStringOrNullCypher25(Map.empty) should be(stringValue("{}"))
  }

  test("Cypher 25: toStringOrNull should stringify a top-level vector") {
    toStringOrNullCypher25(int8Vector(1, 2, 3)) should be(stringValue("vector([1, 2, 3], 3, INTEGER8)"))
  }

  private val entityState = QueryStateHelper.emptyWith(query = mock[QueryContext])

  private def node(id: Long, labels: String*): NodeValue =
    VirtualValues.nodeValue(id, id.toString, stringArray(labels: _*), VirtualValues.EMPTY_MAP)

  private def relationship(id: Long, start: NodeValue, end: NodeValue, relType: String): RelationshipValue =
    VirtualValues.relationshipValue(id, id.toString, start, end, stringValue(relType), VirtualValues.EMPTY_MAP)

  private def toString(orig: Any) = {
    ToStringFunctionCypher5(literal(orig))(CypherRow.empty, QueryStateHelper.empty)
  }

  private def toStringCypher25(orig: Any) = {
    ToStringFunctionCypher25(literal(orig))(CypherRow.empty, entityState)
  }

  private def toStringOrNullCypher5(orig: Any) = {
    ToStringOrNullFunctionCypher5(literal(orig))(CypherRow.empty, QueryStateHelper.empty)
  }

  private def toStringOrNullCypher25(orig: Any) = {
    ToStringOrNullFunctionCypher25(literal(orig))(CypherRow.empty, entityState)
  }
}

/**
 * An AnyValue that matches none of the type checks in CypherFunctions.toString, used to exercise its
 * defensive else-branch: every real Cypher value type is handled by that method today.
 */
private object UnsupportedTestValue extends AnyValue {
  override def writeTo[E <: Exception](writer: AnyValueWriter[E]): Unit = ()
  override def ternaryEquals(other: AnyValue): Equality = Equality.UNDEFINED
  override def map[T](mapper: ValueMapper[T]): T = mapper.mapNoValue()
  override def valueRepresentation(): ValueRepresentation = ValueRepresentation.UNKNOWN
  override protected def equalTo(other: Any): Boolean = other.asInstanceOf[AnyRef] eq this
  override protected def computeHash(): Int = System.identityHashCode(this)
  override def getTypeName: String = "UnsupportedTestValue"
  override def estimatedHeapUsage(): Long = 0L
}
