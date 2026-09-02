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

object IsCoercibleTo extends TypeAssignabilityRelationship {

  def apply(sub: CypherType, sup: CypherType): Boolean = (sub, sup) match {
    // basics
    case (sub, sup) if IsSubtypeOf(sub, sup) => true

    // list types
    case (ListType(subInner, _), ListType(supInner, _)) => IsCoercibleTo(subInner, supInner) && isNullableSub(sub, sup)

    // dynamic union types
    case (ClosedDynamicUnionType(innerTypes), _) => innerTypes.forall(inner => IsCoercibleTo(inner, sup))
    case (_, ClosedDynamicUnionType(innerTypes)) => innerTypes.exists(inner => IsCoercibleTo(sub, inner))

    // record types
    case (sub: RecordType, sup: RecordType) => isAbstractRecordSub(sub, sup)
    case (sub: MapType, sup: RecordType)    => sup.fields.isEmpty && isNullableSub(sub, sup)

    // node reference value types and legacy node types
    case (sub: NodeReferenceValueType, sup: NodeReferenceValueType)     => isNodeReferenceSub(sub, sup)
    case (sub: NodeReferenceValueType, sup: RecordType)                 => isAbstractRecordSub(sub, sup)
    case (NodeReferenceValueType(_, _, _, _), NodeType(_) | MapType(_)) => isNullableSub(sub, sup)
    case (sub: NodeType, sup: NodeReferenceValueType) => isNodeReferenceSub(sub.asNodeReferenceValueType, sup)
    case (NodeType(_), MapType(_))                    => isNullableSub(sub, sup)
    case (NodeType(_), sup: RecordType)               => sup.fields.isEmpty && isNullableSub(sub, sup)

    // relationship reference value types
    case (sub: RelationshipReferenceValueType, sup: RelationshipReferenceValueType) =>
      isRelationshipReferenceSub(sub, sup)
    case (sub: RelationshipReferenceValueType, sup: RecordType) => isAbstractRecordSub(sub, sup)
    case (RelationshipReferenceValueType(_, _, _, _, _, _), RelationshipType(_) | MapType(_)) =>
      isNullableSub(sub, sup)
    case (sub: RelationshipType, sup: RelationshipReferenceValueType) =>
      isRelationshipReferenceSub(sub.asRelationshipReferenceValueType, sup)
    case (RelationshipType(_), MapType(_))      => isNullableSub(sub, sup)
    case (RelationshipType(_), sup: RecordType) => sup.fields.isEmpty && isNullableSub(sub, sup)

    // default
    case (_, _) => false
  }

  override protected inline def opennessRequirementForGraphElementLabels(
    sup: NodeReferenceValueType | RelationshipReferenceValueType
  ): Boolean = true

  override protected inline def opennessRequirementForAbstractRecords(
    sub: AbstractRecordType,
    sup: AbstractRecordType
  ): Boolean = true

  override protected inline def defaultFieldTypeRequirementForAbstractRecords(
    sub: AbstractRecordType,
    sup: AbstractRecordType
  ): Boolean = true

  override protected inline def fillSup(sup: AbstractRecordType, allSubFieldNames: Set[String]): AbstractRecordType =
    sup
}
