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

trait TypeAssignabilityRelationship {
  def apply(sub: CypherType, sup: CypherType): Boolean

  protected def opennessRequirementForGraphElementLabels(sup: NodeReferenceValueType | RelationshipReferenceValueType)
    : Boolean

  protected def isNodeReferenceSub(sub: NodeReferenceValueType, sup: NodeReferenceValueType): Boolean = {
    val labelsOk =
      if (opennessRequirementForGraphElementLabels(sup)) sup.labels subsetOf sub.labels else sup.labels == sub.labels
    labelsOk && isAbstractRecordSub(sub, sup)
  }

  protected def isRelationshipReferenceSub(
    sub: RelationshipReferenceValueType,
    sup: RelationshipReferenceValueType
  ): Boolean = {
    val labelsOk = (opennessRequirementForGraphElementLabels(sup) && sup.label.isEmpty) || sup.label == sub.label
    labelsOk && isAbstractRecordSub(sub, sup) && apply(sub.source, sup.source) && apply(
      sub.destination,
      sup.destination
    )
  }

  protected def isAbstractRecordSub(sub: AbstractRecordType, sup: AbstractRecordType): Boolean = {
    isNullableSub(sub, sup) &&
    opennessRequirementForAbstractRecords(sub, sup) &&
    (sup.fields.keySet subsetOf sub.fields.keySet) &&
    defaultFieldTypeRequirementForAbstractRecords(sub, sup) && {
      val supFilled = fillSup(sup, sub.fields.keySet)
      supFilled.fields.forall {
        case (name, supFieldType) => apply(sub.fields(name), supFieldType)
      }
    }
  }

  protected def opennessRequirementForAbstractRecords(sub: AbstractRecordType, sup: AbstractRecordType): Boolean

  protected def defaultFieldTypeRequirementForAbstractRecords(sub: AbstractRecordType, sup: AbstractRecordType): Boolean

  protected def fillSup(sup: AbstractRecordType, allSubFieldNames: Set[String]): AbstractRecordType

  /* Decides whether sub is a subtype of sup purely regarding the isNullable flag.
   * sub is a subtype of sup if one of the following is true:
   * 1) sup is nullable (it does not matter if sub is nullable or not in this case)
   * 2) if sub is not nullable (it does not matter if sup is nullable or not in this case)
   */
  inline def isNullableSub(sub: CypherType, sup: CypherType): Boolean = sup.isNullable || !sub.isNullable
}
