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
package org.neo4j.cypher.internal.util.symbols

object HasIntersection {
  def unapply(typePair: (CypherType, CypherType)): CypherType = IntersectionOf(typePair._1, typePair._2)
}

object IntersectionOf extends TypeIntersection {

  /**
   * Computes the largest type that is subtype of `a` and subtype of `b`.
   *
   * In other words, when `a` and `b` intersect to X, then
   * 1) Assigning `a` to `b` (`b` to `a`) may involve instances of `a` (`b`) that also of type `b` (`a`)
   *    and, hence, should compile under dynamic typing discipline.
   * 2) At runtime, instance of `a` (`b`) needs to be type checked for X.
   *    Instances that are of type X are also of type `b` (`a`) and, hence, assignable.
   *    Instances that are not of type X shall result in a runtime type error.
   *
   * Note that `IntersectionOf` is commutative.
   */
  def apply(a: CypherType, b: CypherType): CypherType = (a, b) match {
    // shortcuts
    case (CTNothing, _)                    => CTNothing
    case (_, CTNothing)                    => CTNothing
    case (CTNull, o) if !o.isNullable      => CTNothing
    case (o, CTNull) if !o.isNullable      => CTNothing
    case (CTNull, o) /* if o.isNullable */ => CTNull
    case (o, CTNull) /* if o.isNullable */ => CTNull
    case (a: AnyType, o)                   => o.withIsNullable(intersectNullability(a, o))
    case (o, a: AnyType)                   => o.withIsNullable(intersectNullability(a, o))

    // intersection rules
    case (mt: MapType, rt: RecordType)      => nullabilityIntersected(rt, mt)
    case (rt: RecordType, mt: MapType)      => nullabilityIntersected(rt, mt)
    case (rtA: RecordType, rtB: RecordType) => intersectRecordTypes(rtA, rtB)

    case (nt: NodeType, nrt: NodeReferenceValueType)                => nullabilityIntersected(nrt, nt)
    case (nrt: NodeReferenceValueType, nt: NodeType)                => nullabilityIntersected(nrt, nt)
    case (ntA: NodeReferenceValueType, ntB: NodeReferenceValueType) => intersectNodeReferenceValueTypes(ntA, ntB)

    case (rt: RelationshipType, rrt: RelationshipReferenceValueType) => nullabilityIntersected(rrt, rt)
    case (rrt: RelationshipReferenceValueType, rt: RelationshipType) => nullabilityIntersected(rrt, rt)
    case (rtA: RelationshipReferenceValueType, rtB: RelationshipReferenceValueType) =>
      intersectRelationshipReferenceValueTypes(rtA, rtB)

    case (lA: ListType, lB: ListType) => intersectListTypes(lA, lB)

    case (du: ClosedDynamicUnionType, o) => intersectDynamicUnionType(du, o, innerFirst = true)
    // `innerFirst = false` is not actually important here since `IntersectionOf` is commutative
    case (o, du: ClosedDynamicUnionType) => intersectDynamicUnionType(du, o, innerFirst = false)

    case _ if IsSubtypeOf(a, b) => a
    case _ if IsSubtypeOf(b, a) => b

    case _ if a.isNullable && b.isNullable => CTNull
    case _                                 => CTNothing
  }

  override protected inline def rhsDefaultFieldType(rhs: AbstractRecordType): CypherType = rhs.defaultFieldType

  override protected inline def opennessRequirementForRhsNodeLabels(
    rhs: NodeReferenceValueType | RelationshipReferenceValueType,
    lhsExclusiveLabels: Set[String]
  ): Boolean = rhs.isOpen || lhsExclusiveLabels.isEmpty
}
