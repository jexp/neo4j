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

import org.neo4j.cypher.internal.util.InputPosition

trait TypeIntersection {
  protected val pos: InputPosition = InputPosition.NONE

  def apply(a: CypherType, b: CypherType): CypherType

  protected def intersectAbstractRecordTypes(
    a: AbstractRecordType,
    b: AbstractRecordType
  ): RecordType | NullType | NothingType = {
    val defaultFieldType = apply(a.defaultFieldType, rhsDefaultFieldType(b))
    val aExclusiveFieldNames = a.fields.keySet -- b.fields.keySet
    val bExclusiveFieldNames = b.fields.keySet -- a.fields.keySet
    val commonFieldNames = a.fields.keySet intersect b.fields.keySet

    val aExclusiveFields =
      aExclusiveFieldNames.map(name => name -> apply(a.fieldType(name), rhsDefaultFieldType(b)))
    val bExclusiveFields =
      bExclusiveFieldNames.map(name => name -> apply(b.fieldType(name), a.defaultFieldType))
    val commonFields = commonFieldNames.map(name =>
      name -> apply(a.fieldType(name), b.fieldType(name))
    )
    val fields = (aExclusiveFields union bExclusiveFields union commonFields).toMap

    if (fields.exists(_._2.isNothing)) {
      if (intersectNullability(a, b)) {
        CTNull
      } else {
        CTNothing
      }
    } else {
      RecordType(fields, defaultFieldType, intersectNullability(a, b))(pos)
    }
  }

  protected def rhsDefaultFieldType(rhs: AbstractRecordType): CypherType

  protected inline def intersectRecordTypes(
    a: RecordType,
    b: RecordType
  ): RecordType | NullType | NothingType = intersectAbstractRecordTypes(a, b)

  protected inline def intersectNodeReferenceValueTypes(
    a: NodeReferenceValueType,
    b: NodeReferenceValueType
  ): NodeReferenceValueType | NullType | NothingType = {
    val aExclusiveLabels = a.labels -- b.labels
    val bExclusiveLabels = b.labels -- a.labels
    intersectAbstractRecordTypes(a, b) match {
      case rt: RecordType
        if opennessRequirementForRhsNodeLabels(b, aExclusiveLabels) && (a.isOpen || bExclusiveLabels.isEmpty) =>
        NodeReferenceValueType(a.labels union b.labels, rt.fields, rt.defaultFieldType, rt.isNullable)(pos)
      case _ =>
        if (intersectNullability(a, b)) {
          CTNull
        } else {
          CTNothing
        }
    }
  }

  protected def opennessRequirementForRhsNodeLabels(
    rhs: NodeReferenceValueType | RelationshipReferenceValueType,
    lhsExclusiveLabels: Set[String]
  ): Boolean

  protected inline def intersectRelationshipReferenceValueTypes(
    a: RelationshipReferenceValueType,
    b: RelationshipReferenceValueType
  ): RelationshipReferenceValueType | NullType | NothingType = {
    val source = intersectNodeReferenceValueTypes(a.source, b.source)
    val destination = intersectNodeReferenceValueTypes(a.destination, b.destination)
    val record = intersectAbstractRecordTypes(a, b)
    (record, source, destination) match {
      case (rt: RecordType, s: NodeReferenceValueType, d: NodeReferenceValueType)
        if (a.isOpen || b.isOpen || a.label == b.label) =>
        RelationshipReferenceValueType(
          a.label.orElse(b.label),
          rt.fields,
          rt.defaultFieldType,
          s,
          d,
          rt.isNullable
        )(pos)
      case _ =>
        if (intersectNullability(a, b)) {
          CTNull
        } else {
          CTNothing
        }
    }
  }

  protected inline def intersectListTypes(a: ListType, b: ListType): ListType = {
    ListType(apply(a.innerType, b.innerType), intersectNullability(a, b))(pos)
  }

  protected inline def intersectDynamicUnionType(
    du: ClosedDynamicUnionType,
    other: CypherType,
    innerFirst: Boolean
  ): CypherType = {
    du.innerTypes.map(innerType =>
      if (innerFirst) apply(innerType, other) else apply(other, innerType)
    ).filter(!_.isNothing) match {
      case set if set.isEmpty   => CTNothing
      case set if set.size == 1 => set.head
      case set                  => CypherType.normalizeTypes(ClosedDynamicUnionType(set)(pos))
    }
  }

  /*
   * Returns the first type set nullable only nullable if both types (first and second) are nullable.
   */
  protected inline def nullabilityIntersected(t: CypherType, o: CypherType): CypherType =
    t.withIsNullable(intersectNullability(t, o))

  /*
   * Returns true if both types (first and second) are nullable.
   */
  protected inline def intersectNullability(a: CypherType, b: CypherType): Boolean = a.isNullable && b.isNullable
}
