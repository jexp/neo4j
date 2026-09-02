/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.util.types

import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTNothing
import org.neo4j.cypher.internal.util.symbols.CTNull
import org.neo4j.cypher.internal.util.symbols.CoercibleIntersectionOf
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.symbols.IsCoercibleTo
import org.neo4j.cypher.internal.util.symbols.IsEqualTo
import org.neo4j.cypher.internal.util.symbols.IsSubtypeOf
import org.neo4j.cypher.internal.util.symbols.MapType
import org.neo4j.cypher.internal.util.symbols.NodeReferenceValueType
import org.neo4j.cypher.internal.util.symbols.NodeType
import org.neo4j.cypher.internal.util.symbols.RecordType
import org.neo4j.cypher.internal.util.symbols.RelationshipType
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class CoercibleIntersectionOfTest extends CypherTypeTestSuite {

  for (l <- Lattice.allLattices) {
    test(s"lattice: ${l.name}") {
      shouldIntersectAtMeetOfLattice(l)
    }
  }

  private def shouldIntersectAtMeetOfLattice(lattice: Lattice): Unit = {
    val meets = computeTaggedMeets(lattice.edges).map {
      case (a, b, Some((mt: MapType, c)))          => (a, b, Some((mt.asRecordType, c)))
      case (a, b, Some((nt: NodeType, c)))         => (a, b, Some((nt.asNodeReferenceValueType, c)))
      case (a, b, Some((rt: RelationshipType, c))) => (a, b, Some((rt.asRelationshipReferenceValueType, c)))
      case x                                       => x
    }
    for ((from, to, meetOpt) <- meets) {
      meetOpt match {
        case Some((meet, withCoercion)) =>
          checkCoercibleIntersectionOf(from, to)(meet)
        case None =>
          checkNotCoercibleIntersect(from, to)
      }
    }
    val allTypes = lattice.edgesWithCoercion.flatMap {
      case (a, b) => Seq(a, b)
    }
    for (t <- allTypes) {
      checkInvariants(t)
    }
  }

  private def checkCoercibleIntersectionOf(from: CypherType, to: CypherType)(c: CypherType): Unit = {
    assertCoercibleIntersectionOf(from, to)(c)
    assertIsCoercibleTo(c, from)
    assertIsCoercibleTo(c, to)
  }

  private def checkNotCoercibleIntersect(from: CypherType, to: CypherType): Unit = {
    val aNotNull = from.notNull
    val bNotNull = to.notNull
    val aNull = from.nullable
    val bNull = to.nullable
    assertCoercibleIntersectionOf(aNotNull, bNotNull)(CTNothing)
    assertCoercibleIntersectionOf(aNotNull, bNull)(CTNothing)
    assertCoercibleIntersectionOf(aNull, bNotNull)(CTNothing)
    assertCoercibleIntersectionOf(aNull, bNull)(CTNull)
  }

  private def checkInvariants(a: CypherType): Unit = {
    assertCoercibleIntersectionOf(a, CTAny)(a)
    assertCoercibleIntersectionOf(a, a)(a)
    assertCoercibleIntersectionOf(a, CTNothing)(CTNothing)
    if (a.isNullable) {
      assertCoercibleIntersectionOf(a, CTNull)(CTNull)
    } else {
      assertCoercibleIntersectionOf(a, CTNull)(CTNothing)
    }
    val listOfA = l(a)
    if (notAny(a) && noListType(a) && !IsSubtypeOf(listOfA, a)) {
      if (a.isNullable) {
        assertCoercibleIntersectionOf(a, listOfA)(CTNull)
      } else {
        assertCoercibleIntersectionOf(a, listOfA)(CTNothing)
      }
    }
    if (isAny(a)) {
      if (a.isNullable) {
        assertCoercibleIntersectionOf(a, listOfA)(listOfA)
      } else {
        assertCoercibleIntersectionOf(a, listOfA)(listOfA.notNull)
      }
    }
    val recordOfA = rt("x" :: a)
    if (
      notAny(a) && noRecordType(a) && noNodeReferenceValueType(a) && noRelationshipReferenceValueType(
        a
      ) && !IsSubtypeOf(recordOfA, a)
    ) {
      if (a.isNullable) {
        assertCoercibleIntersectionOf(a, recordOfA)(CTNull)
      } else {
        assertCoercibleIntersectionOf(a, recordOfA)(CTNothing)
      }
    }
    if (isAny(a)) {
      if (a.isNullable) {
        assertCoercibleIntersectionOf(a, recordOfA)(recordOfA)
      } else {
        assertCoercibleIntersectionOf(a, recordOfA)(recordOfA.notNull)
      }
    }
  }

  private def assertCoercibleIntersectionOf(from: CypherType, to: CypherType)(expected: CypherType): Unit = {
    val actual = CoercibleIntersectionOf(from, to)
    if (!IsEqualTo(actual, expected)) {
      CoercibleIntersectionOf(from, to)
      fail(s"""${from.description} and ${to.description} should coercible intersect to ${expected.description}, but coercible intersected to ${actual.description}
              |from: ${pprint.apply(from)}
              |to:   ${pprint.apply(to)}
              |
              |expected: ${pprint.apply(expected)}
              |actual:   ${pprint.apply(actual)}""".stripMargin)
    }
  }

  private def assertIsCoercibleTo(from: CypherType, to: CypherType): Unit = {
    if (!IsCoercibleTo(from, to)) {
      fail(
        s"""${from.description} should be coercible to ${to.description}, but is not
           |from: ${pprint.apply(from)}
           |to:   ${pprint.apply(to)}""".stripMargin
      )
    }
  }
}
