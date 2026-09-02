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

object CoercibleIntersectionOf extends TypeIntersection {

  /**
   * Computes a type that the largest subtype of `from` that is coercible to `to`.
   *
   * In other words, when `from` and `to` coercibly intersect to X, then
   * 1) Assigning `from` to `to` may involve instances of `from` that are coercible to `to`
   *    and, hence, should compile under dynamic typing discipline.
   * 2) At runtime, instance of `from` needs to be type checked for X.
   *    Instances that are of type X are coercible to `to` and, hence, assignable.
   *    Instances that are not of type X shall result in a runtime type error.
   *
   * Note that `CoercibleIntersectionOf` is not commutative.
   */
  def apply(from: CypherType, to: CypherType): CypherType = (from, to) match {
    // regular intersection
    case HasIntersection(t) if t != CTNull => t

    // direct coercibility
    case _ if IsCoercibleTo(from, to) => from

    // coercion rules
    case (mt: MapType, rt: RecordType)          => nullabilityIntersected(rt, mt)
    case (rt: RecordType, mt: MapType)          => nullabilityIntersected(rt, mt)
    case (rtFrom: RecordType, rtTo: RecordType) => intersectRecordTypes(rtFrom, rtTo)

    case (nt: NodeType, rt: RecordType)                => intersectNodeTypeAndRecordType(nt, rt)
    case (nrt: NodeReferenceValueType, rt: RecordType) => intersectNodeReferenceValueTypeAndRecordType(nrt, rt)

    case (relT: RelationshipType, rt: RecordType) =>
      intersectRelationshipTypeAndRecordType(relT, rt)
    case (rrt: RelationshipReferenceValueType, rt: RecordType) =>
      intersectRelationshipReferenceValueTypeAndRecordType(rrt, rt)

    case (nt: NodeType, nrt: NodeReferenceValueType)                => nullabilityIntersected(nrt, nt)
    case (nrt: NodeReferenceValueType, nt: NodeType)                => nullabilityIntersected(nrt, nt)
    case (ntA: NodeReferenceValueType, ntB: NodeReferenceValueType) => intersectNodeReferenceValueTypes(ntA, ntB)

    case (rt: RelationshipType, rrt: RelationshipReferenceValueType) => nullabilityIntersected(rrt, rt)
    case (rrt: RelationshipReferenceValueType, rt: RelationshipType) => nullabilityIntersected(rrt, rt)
    case (rtA: RelationshipReferenceValueType, rtB: RelationshipReferenceValueType) =>
      intersectRelationshipReferenceValueTypes(rtA, rtB)

    case (lA: ListType, lB: ListType) => intersectListTypes(lA, lB)

    case (du: ClosedDynamicUnionType, o) => intersectDynamicUnionType(du, o, innerFirst = true)
    // `innerFirst = false` is important here since `CoercibleIntersectionOf` is not commutative
    case (o, du: ClosedDynamicUnionType) => intersectDynamicUnionType(du, o, innerFirst = false)

    case _ if from.isNullable && to.isNullable => CTNull
    case _                                     => CTNothing
  }

  override protected inline def rhsDefaultFieldType(rhs: AbstractRecordType): CypherType = CTAny

  override protected inline def opennessRequirementForRhsNodeLabels(
    rhs: NodeReferenceValueType | RelationshipReferenceValueType,
    lhsExclusiveLabels: Set[String]
  ): Boolean = true

  private inline def intersectNodeTypeAndRecordType(fromNt: NodeType, toRt: RecordType): NodeReferenceValueType = {
    NodeReferenceValueType(Set.empty, toRt.fields, rhsDefaultFieldType(toRt), intersectNullability(fromNt, toRt))(pos)
  }

  private inline def intersectNodeReferenceValueTypeAndRecordType(
    fromNt: NodeReferenceValueType,
    toRt: RecordType
  ): NodeReferenceValueType | NullType | NothingType = {
    intersectAbstractRecordTypes(fromNt, toRt) match {
      case rtIntersection: RecordType =>
        NodeReferenceValueType(
          fromNt.labels,
          rtIntersection.fields,
          rtIntersection.defaultFieldType,
          rtIntersection.isNullable
        )(pos)
      case n: (NullType | NothingType) => n
    }
  }

  private inline def intersectRelationshipTypeAndRecordType(
    fromRelT: RelationshipType,
    toRt: RecordType
  ): RelationshipReferenceValueType = {
    val endpoint = NodeReferenceValueType.any(false)(pos)
    RelationshipReferenceValueType(
      Option.empty,
      toRt.fields,
      rhsDefaultFieldType(toRt),
      endpoint,
      endpoint,
      intersectNullability(fromRelT, toRt)
    )(pos)
  }

  private inline def intersectRelationshipReferenceValueTypeAndRecordType(
    fromRrt: RelationshipReferenceValueType,
    toRt: RecordType
  ): RelationshipReferenceValueType | NullType | NothingType = {
    intersectAbstractRecordTypes(fromRrt, toRt) match {
      case rtIntersection: RecordType =>
        RelationshipReferenceValueType(
          fromRrt.label,
          rtIntersection.fields,
          rtIntersection.defaultFieldType,
          fromRrt.source,
          fromRrt.destination,
          rtIntersection.isNullable
        )(pos)
      case n: (NullType | NothingType) => n
    }
  }
}
