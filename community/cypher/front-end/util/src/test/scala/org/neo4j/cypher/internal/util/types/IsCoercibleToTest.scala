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
import org.neo4j.cypher.internal.util.symbols.CTAnyNotNull
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTNothing
import org.neo4j.cypher.internal.util.symbols.CTNull
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.symbols.IsCoercibleTo
import org.neo4j.cypher.internal.util.symbols.IsEqualTo
import org.neo4j.cypher.internal.util.symbols.IsSubtypeOf

import scala.util.Random

class IsCoercibleToTest extends CypherTypeTestSuite {

  for (l <- Lattice.allLattices) {
    test(s"lattice: ${l.name}") {
      shouldBeCoercibleSubtypeLattice(l)
    }
  }

  /*
   * checks and assertions
   */

  private def shouldBeCoercibleSubtypeLattice(lattice: Lattice): Unit = {
    val subtypeOfEdges = lattice.edgesWithoutCoercion
    val allCoercibleRelationships =
      for {
        (sub, sup) <- transitiveClosure(lattice.coercionEdges)
        ideal = computeIdeal(sub, subtypeOfEdges)
        filter = computeFilter(sup, subtypeOfEdges)
        subC <- ideal
        supC <- filter
      } yield {
        // Note that the above selection of subC and supC does not guarantee that
        // IsCoercibleTo requires the coercible edge (sub, sup), since
        // subC may be a subtype of supC (without any coercion) independently of (sub, sup).
        // We could filter these out (`if !IsSubtypeOf(subC, supC)`), but we do not bother to
        (subC, supC)
      }

    for ((sub, sup) <- allCoercibleRelationships) {
      checkIsCoercibleTo(sub, sup)
    }
    val allTypes = lattice.allTypes
    // for the negative tests we sample 10 from each side to keep the test runtime under control
    for {
      a <- Random.shuffle(allTypes).take(10)
      b <- Random.shuffle(allTypes).take(10)
      if a != b && !allCoercibleRelationships.contains((a, b)) && !IsSubtypeOf(a, b)
    } checkIsNotCoercibleTo(a, b)
  }

  private def checkIsCoercibleTo(sub: CypherType, sup: CypherType, nestingLevel: Int = 0): Unit = {
    assertIsCoercibleTo(sub, sup)
    checkNullabilityInvariantForCoercibles(sub, sup)
    // given SUB <: SUP
    // check for list covariance
    {
      if (nestingLevel < 3) {
        // LIST<SUB> <: LIST<SUP>
        checkIsCoercibleTo(l(sub), l(sup), nestingLevel + 1)
        // LIST<SUB - NULL> <: LIST<SUP + NULL>
        checkIsCoercibleTo(l(sub.notNull), l(sup.nullable), nestingLevel + 1)
        // but not LIST<SUB + NULL> <: LIST<SUP - NULL>
        checkIsNotCoercibleTo(l(sub.nullable), l(sup.notNull), nestingLevel + 1)
      }
      // if SUB is not a LIST or the nothing type, then not SUB <: LIST<SUP>
      if (noListType(sub) && sub != CTNothing && sub != CTNull) checkIsNotCoercibleTo(sub, l(sup), nestingLevel + 1)
      // if SUP is not a RECORD or the nothing type, then not LIST<SUB> <: SUP
      if (noListType(sup) && sup != CTAny) checkIsNotCoercibleTo(l(sub), sup, nestingLevel + 1)
    }
    // check for record covariance
    {
      if (nestingLevel < 3) {
        // { x :: SUB } <: { x :: SUP }
        checkIsCoercibleTo(rt("x" :: sub), rt("x" :: sup), nestingLevel + 1)
        // { x :: SUB, y :: BOOLEAN, z :: SUB } <: { x :: SUP, y :: BOOLEAN, z :: SUP }
        checkIsCoercibleTo(
          rt("x" :: sub, "y" :: CTBoolean, "z" :: sub),
          rt("x" :: sup, "y" :: CTBoolean, "z" :: sup),
          nestingLevel + 1
        )
        // { x :: SUB - NULL } <: { x :: SUP + NULL }
        checkIsCoercibleTo(rt("x" :: sub.notNull), rt("x" :: sup.nullable), nestingLevel + 1)
        // but not { x :: SUB + NULL } <: { x :: SUP - NULL }
        checkIsNotCoercibleTo(rt("x" :: sub.nullable), rt("x" :: sup.notNull), nestingLevel + 1)
      }
      // if SUB is not a RECORD or the nothing type, then not SUB <: { x :: SUP }
      if (noRecordType(sub) && sub != CTNothing && sub != CTNull)
        checkIsNotCoercibleTo(sub, rt("x" :: sup), nestingLevel + 1)
      // if SUP is not a RECORD or the nothing type, then not { x :: SUB } <: SUP
      if (noRecordType(sup) && sup != CTAny) checkIsNotCoercibleTo(rt("x" :: sub), sup, nestingLevel + 1)
    }
  }

  private def checkNullabilityInvariantForCoercibles(sub: CypherType, sup: CypherType): Unit = {
    assertIsCoercibleTo(sub.notNull, sup.nullable)
    assertIsCoercibleTo(sub.notNull, sup.notNull)
    assertIsCoercibleTo(sub.nullable, sup.nullable)
    assertIsNotCoercibleTo(sub.nullable, sup.notNull)
  }

  private def checkIsNotCoercibleTo(sub: CypherType, sup: CypherType, nestingLevel: Int = 0): Unit = {
    assertIsNotCoercibleTo(sub, sup)
    checkNullabilityInvariantForNonCoercibles(sub, sup)
    // check for list covariance
    if (nestingLevel < 3) checkIsNotCoercibleTo(l(sub), l(sup), nestingLevel + 1)
    // check for record covariance
    if (nestingLevel < 3) {
      checkIsNotCoercibleTo(rt("x" :: sub), rt("x" :: sup), nestingLevel + 1)
      checkIsNotCoercibleTo(rt("x" :: sub, "y" :: sup), rt("x" :: sup, "y" :: sub), nestingLevel + 1)
      checkIsNotCoercibleTo(
        rt("x" :: sub, "y" :: CTBoolean, "z" :: sub),
        rt("x" :: sup, "y" :: CTBoolean, "z" :: sup),
        nestingLevel + 1
      )
    }
  }

  private def checkNullabilityInvariantForNonCoercibles(sub: CypherType, sup: CypherType): Unit = {
    if (!IsEqualTo(sub.nullable, sup.nullable)) {
      if (sub.notNull != CTNothing) {
        if (sup.nullable != CTAny) assertIsNotCoercibleTo(sub.notNull, sup.nullable)
        if (sup.notNull != CTAnyNotNull) assertIsNotCoercibleTo(sub.notNull, sup.notNull)
      }
      if (sub.nullable != CTNull) {
        if (sup.nullable != CTAny) assertIsNotCoercibleTo(sub.nullable, sup.nullable)
        if (sup.notNull != CTAnyNotNull) assertIsNotCoercibleTo(sub.nullable, sup.notNull)
      }
    }
  }

  private def assertIsCoercibleTo(sub: CypherType, sup: CypherType): Unit = {
    if (!IsCoercibleTo(sub, sup)) {
      fail(
        s"""${sub.description} should be coercible to ${sup.description}, but is not
           |sub: ${pprint.apply(sub)}
           |sup: ${pprint.apply(sup)}""".stripMargin
      )
    }
  }

  private def assertIsNotCoercibleTo(sub: CypherType, sup: CypherType): Unit = {
    if (IsCoercibleTo(sub, sup)) {
      fail(s"""${sub.description} should not be coercible to ${sup.description}, but is
              |sub: ${pprint.apply(sub)}
              |sup: ${pprint.apply(sup)}""".stripMargin)
    }
  }
}
