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
package org.neo4j.internal.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.common.EntityType.RELATIONSHIP;
import static org.neo4j.internal.schema.IndexType.FULLTEXT;
import static org.neo4j.internal.schema.IndexType.LOOKUP;
import static org.neo4j.internal.schema.IndexType.POINT;
import static org.neo4j.internal.schema.IndexType.RANGE;
import static org.neo4j.internal.schema.IndexType.TEXT;
import static org.neo4j.internal.schema.IndexType.VECTOR;
import static org.neo4j.internal.schema.SchemaDescriptors.ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR;
import static org.neo4j.internal.schema.SchemaDescriptors.ANY_TOKEN_RELATIONSHIP_SCHEMA_DESCRIPTOR;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.internal.schema.constraints.PropertyTypeSet;
import org.neo4j.internal.schema.constraints.SchemaValueType;
import org.neo4j.test.InMemoryTokens;

class SchemaRuleTest {
    private final LabelSchemaDescriptor labelSchema = SchemaDescriptors.forLabel(1, 2, 3);
    private final LabelSchemaDescriptor labelSchema2 = SchemaDescriptors.forLabel(0, 0, 1);
    private final RelationTypeSchemaDescriptor relTypeSchema = SchemaDescriptors.forRelType(1, 2, 3);
    private final SemanticSearchSchemaDescriptor nodeSemanticSearchSchema =
            SchemaDescriptors.forSemanticSearch(NODE, new int[] {1, 2}, new int[] {1, 2});
    private final SemanticSearchSchemaDescriptor relSemanticSearchSchema =
            SchemaDescriptors.forSemanticSearch(RELATIONSHIP, new int[] {1, 2}, new int[] {1, 2});
    private final SemanticSearchSchemaDescriptor nodeSemanticSearchSchema2 =
            SchemaDescriptors.forSemanticSearch(NODE, new int[] {0, 1}, new int[] {0, 1});
    private final AnyTokenSchemaDescriptor allLabelsSchema = ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR;
    private final AnyTokenSchemaDescriptor allRelTypesSchema = ANY_TOKEN_RELATIONSHIP_SCHEMA_DESCRIPTOR;
    private final LabelSchemaDescriptor labelSinglePropSchema = SchemaDescriptors.forLabel(1, 2);
    private final RelationTypeSchemaDescriptor relTypeSinglePropSchema = SchemaDescriptors.forRelType(1, 2);
    private final IndexPrototype rangeLabelPrototype =
            IndexPrototype.forSchema(labelSchema).withIndexType(RANGE);
    private final IndexPrototype rangeLabelPrototype2 =
            IndexPrototype.forSchema(labelSchema2).withIndexType(RANGE);
    private final IndexPrototype rangeLabelUniquePrototype =
            IndexPrototype.uniqueForSchema(labelSchema).withIndexType(RANGE);
    private final IndexPrototype rangeRelTypePrototype =
            IndexPrototype.forSchema(relTypeSchema).withIndexType(RANGE);
    private final IndexPrototype rangeRelTypeUniquePrototype =
            IndexPrototype.uniqueForSchema(relTypeSchema).withIndexType(RANGE);
    private final IndexPrototype nodeFtsPrototype =
            IndexPrototype.forSchema(nodeSemanticSearchSchema).withIndexType(FULLTEXT);
    private final IndexPrototype relFtsPrototype =
            IndexPrototype.forSchema(relSemanticSearchSchema).withIndexType(FULLTEXT);
    private final IndexPrototype nodeFtsPrototype2 =
            IndexPrototype.forSchema(nodeSemanticSearchSchema2).withIndexType(FULLTEXT);
    private final IndexPrototype allLabelsPrototype =
            IndexPrototype.forSchema(allLabelsSchema).withIndexType(LOOKUP);
    private final IndexPrototype allRelTypesPrototype =
            IndexPrototype.forSchema(allRelTypesSchema).withIndexType(LOOKUP);
    private final IndexPrototype textLabelPrototype =
            IndexPrototype.forSchema(labelSinglePropSchema).withIndexType(TEXT);
    private final IndexPrototype textRelTypePrototype =
            IndexPrototype.forSchema(relTypeSinglePropSchema).withIndexType(TEXT);
    private final IndexPrototype pointLabelPrototype =
            IndexPrototype.forSchema(labelSinglePropSchema).withIndexType(POINT);
    private final IndexPrototype pointRelTypePrototype =
            IndexPrototype.forSchema(relTypeSinglePropSchema).withIndexType(POINT);
    private final IndexPrototype vectorLabelPrototype =
            IndexPrototype.forSchema(nodeSemanticSearchSchema).withIndexType(VECTOR);
    private final IndexPrototype vectorLabelPrototype2 =
            IndexPrototype.forSchema(nodeSemanticSearchSchema2).withIndexType(VECTOR);
    private final IndexPrototype vectorRelTypePrototype =
            IndexPrototype.forSchema(relSemanticSearchSchema).withIndexType(VECTOR);
    private final IndexPrototype rangeLabelPrototypeNamed = rangeLabelPrototype.withName("rangeLabelPrototypeNamed");
    private final IndexPrototype rangeLabelPrototype2Named =
            IndexPrototype.forSchema(labelSchema2).withName("labelPrototype2Named");
    private final IndexPrototype rangeLabelUniquePrototypeNamed =
            rangeLabelUniquePrototype.withName("rangeLabelUniquePrototypeNamed");
    private final IndexPrototype rangeRelTypePrototypeNamed =
            rangeRelTypePrototype.withName("rangeRelTypePrototypeNamed");
    private final IndexPrototype rangeRelTypeUniquePrototypeNamed =
            rangeRelTypeUniquePrototype.withName("rangeRelTypeUniquePrototypeNamed");
    private final IndexPrototype nodeFtsPrototypeNamed = IndexPrototype.forSchema(nodeSemanticSearchSchema)
            .withIndexType(FULLTEXT)
            .withName("nodeFtsPrototypeNamed");
    private final IndexPrototype relFtsPrototypeNamed = IndexPrototype.forSchema(relSemanticSearchSchema)
            .withIndexType(FULLTEXT)
            .withName("relFtsPrototypeNamed");
    private final IndexPrototype nodeFtsPrototype2Named = IndexPrototype.forSchema(nodeSemanticSearchSchema2)
            .withIndexType(FULLTEXT)
            .withName("nodeFtsPrototype2Named");
    private final IndexPrototype allLabelsPrototypeNamed =
            IndexPrototype.forSchema(allLabelsSchema).withIndexType(LOOKUP).withName("allLabelsPrototypeNamed");
    private final IndexPrototype allRelTypesPrototypeNamed =
            IndexPrototype.forSchema(allRelTypesSchema).withIndexType(LOOKUP).withName("allRelTypesPrototypeNamed");
    private final IndexPrototype textLabelPrototypeNamed = textLabelPrototype.withName("textLabelPrototypeNamed");
    private final IndexPrototype textRelTypePrototypeNamed = textRelTypePrototype.withName("textRelTypePrototypeNamed");
    private final IndexPrototype pointLabelPrototypeNamed = pointLabelPrototype.withName("pointLabelPrototypeNamed");
    private final IndexPrototype pointRelTypePrototypeNamed =
            pointRelTypePrototype.withName("pointRelTypePrototypeNamed");
    private final IndexPrototype vectorLabelPrototypeNamed = vectorLabelPrototype.withName("vectorLabelPrototypeNamed");
    private final IndexPrototype vectorLabelPrototype2Named =
            vectorLabelPrototype2.withName("vectorLabelPrototype2Named");
    private final IndexPrototype vectorRelTypePrototypeNamed =
            vectorRelTypePrototype.withName("vectorRelTypePrototypeNamed");
    private final IndexDescriptor rangeLabelIndexNamed =
            rangeLabelPrototypeNamed.withName("rangeLabelIndexNamed").materialise(1);
    private final IndexDescriptor rangeLabelIndex2Named =
            rangeLabelPrototype2Named.withName("labelIndex2Named").materialise(2);
    private final IndexDescriptor rangeLabelUniqueIndexNamed = rangeLabelUniquePrototypeNamed
            .withName("rangeLabelUniqueIndexNamed")
            .materialise(3);
    private final IndexDescriptor rangeRelTypeIndexNamed =
            rangeRelTypePrototypeNamed.withName("rangeRelTypeIndexNamed").materialise(4);
    private final IndexDescriptor rangeRelTypeUniqueIndexNamed = rangeRelTypeUniquePrototypeNamed
            .withName("rangeRelTypeUniqueIndexNamed")
            .materialise(5);
    private final IndexDescriptor nodeFtsIndexNamed =
            nodeFtsPrototypeNamed.withName("nodeFtsIndexNamed").materialise(6);
    private final IndexDescriptor relFtsIndexNamed =
            relFtsPrototypeNamed.withName("relFtsIndexNamed").materialise(7);
    private final IndexDescriptor nodeFtsIndex2Named =
            nodeFtsPrototype2Named.withName("nodeFtsIndex2Named").materialise(8);
    private final IndexDescriptor allLabelsIndexNamed =
            allLabelsPrototypeNamed.withName("allLabelsIndexNamed").materialise(9);
    private final IndexDescriptor allRelTypesIndexNamed =
            allRelTypesPrototypeNamed.withName("allRelTypesIndexNamed").materialise(10);
    private final IndexDescriptor textLabelIndexNamed =
            textLabelPrototypeNamed.withName("textLabelIndexNamed").materialise(11);
    private final IndexDescriptor textRelTypeIndexNamed =
            textRelTypePrototypeNamed.withName("textRelTypeIndexNamed").materialise(12);
    private final IndexDescriptor pointLabelIndexNamed =
            pointLabelPrototypeNamed.withName("pointLabelIndexNamed").materialise(13);
    private final IndexDescriptor pointRelTypeIndexNamed =
            pointRelTypePrototypeNamed.withName("pointRelTypeIndexNamed").materialise(14);
    private final IndexDescriptor indexBelongingToConstraint = rangeLabelUniquePrototypeNamed
            .withName("indexBelongingToConstraint")
            .materialise(15)
            .withOwningConstraintId(1);
    private final IndexDescriptor vectorLabelIndexNamed =
            vectorLabelPrototypeNamed.withName("vectorLabelIndexNamed").materialise(16);
    private final IndexDescriptor vectorLabelIndex2Named =
            vectorLabelPrototype2Named.withName("vectorLabelIndex2Named").materialise(17);
    private final IndexDescriptor vectorRelTypeIndexNamed =
            vectorRelTypePrototypeNamed.withName("vectorRelTypeIndexNamed").materialise(18);
    private final ConstraintDescriptor uniqueLabelConstraint =
            ConstraintDescriptorFactory.uniqueForSchema(labelSchema, RANGE);
    private final ConstraintDescriptor uniqueRelTypeConstraint =
            ConstraintDescriptorFactory.uniqueForSchema(relTypeSchema, RANGE);
    private final ConstraintDescriptor existsLabelConstraint =
            ConstraintDescriptorFactory.existsForSchema(labelSchema, false);
    private final ConstraintDescriptor nodeKeyConstraint = ConstraintDescriptorFactory.keyForSchema(labelSchema, RANGE);
    private final ConstraintDescriptor relKeyConstraint =
            ConstraintDescriptorFactory.keyForSchema(relTypeSchema, RANGE);
    private final ConstraintDescriptor existsRelTypeConstraint =
            ConstraintDescriptorFactory.existsForSchema(relTypeSchema, false);
    private final ConstraintDescriptor uniqueLabelConstraint2 =
            ConstraintDescriptorFactory.uniqueForSchema(labelSchema2);
    private final ConstraintDescriptor uniqueLabelConstraintNamed = uniqueLabelConstraint
            .withName("uniqueLabelConstraintNamed")
            .withId(1)
            .withOwnedIndexId(1);
    private final ConstraintDescriptor uniqueRelTypeConstraintNamed = uniqueRelTypeConstraint
            .withName("uniqueRelTypeConstraintNamed")
            .withId(7)
            .withOwnedIndexId(1);
    private final ConstraintDescriptor existsLabelConstraintNamed =
            existsLabelConstraint.withName("existsLabelConstraintNamed").withId(2);
    private final ConstraintDescriptor nodeKeyConstraintNamed =
            nodeKeyConstraint.withName("nodeKeyConstraintNamed").withId(3).withOwnedIndexId(3);
    private final ConstraintDescriptor relKeyConstraintNamed =
            relKeyConstraint.withName("relKeyConstraintNamed").withId(6).withOwnedIndexId(4);
    private final ConstraintDescriptor existsRelTypeConstraintNamed =
            existsRelTypeConstraint.withName("existsRelTypeConstraintNamed").withId(4);
    private final ConstraintDescriptor uniqueLabelConstraint2Named = uniqueLabelConstraint2
            .withName("uniqueLabelConstraint2Named")
            .withId(5)
            .withOwnedIndexId(5);
    private final ConstraintDescriptor nodeTypeConstraintIntBool = ConstraintDescriptorFactory.typeForSchema(
            labelSchema, PropertyTypeSet.of(SchemaValueType.INTEGER, SchemaValueType.BOOLEAN), false);
    private final ConstraintDescriptor nodeTypeConstraintBoolInt = ConstraintDescriptorFactory.typeForSchema(
            labelSchema, PropertyTypeSet.of(SchemaValueType.BOOLEAN, SchemaValueType.INTEGER), false);
    private final ConstraintDescriptor nodeTypeConstraintBoolString = ConstraintDescriptorFactory.typeForSchema(
            labelSchema, PropertyTypeSet.of(SchemaValueType.BOOLEAN, SchemaValueType.STRING), false);
    private final ConstraintDescriptor relationshipTypeConstraintIntBool = ConstraintDescriptorFactory.typeForSchema(
            relTypeSchema, PropertyTypeSet.of(SchemaValueType.BOOLEAN, SchemaValueType.STRING), false);
    private final ConstraintDescriptor relationshipTypeConstraintBoolInt = ConstraintDescriptorFactory.typeForSchema(
            relTypeSchema, PropertyTypeSet.of(SchemaValueType.BOOLEAN, SchemaValueType.STRING), false);
    private final ConstraintDescriptor namedNodeTypeConstraint = ConstraintDescriptorFactory.typeForSchema(
                    labelSchema, PropertyTypeSet.of(SchemaValueType.STRING), false)
            .withName("namedNodeTypeConstraint")
            .withId(10);
    private final ConstraintDescriptor namedRelationshipTypeConstraint = ConstraintDescriptorFactory.typeForSchema(
                    relTypeSchema, PropertyTypeSet.of(SchemaValueType.STRING), false)
            .withName("namedRelationshipTypeConstraint")
            .withId(11);
    private final ConstraintDescriptor relationshipEndpointLabelStartConstraint =
            ConstraintDescriptorFactory.relationshipEndpointLabelForRelType(0, 0, EndpointType.START);
    private final ConstraintDescriptor relationshipEndpointLabelStartAnotherLabelConstraint =
            ConstraintDescriptorFactory.relationshipEndpointLabelForRelType(0, 1, EndpointType.START);
    private final ConstraintDescriptor relationshipEndpointLabelStartAnotherRelTypeConstraint =
            ConstraintDescriptorFactory.relationshipEndpointLabelForRelType(1, 0, EndpointType.START);
    private final ConstraintDescriptor relationshipEndpointLabelEndConstraint =
            ConstraintDescriptorFactory.relationshipEndpointLabelForRelType(0, 0, EndpointType.END);
    private final ConstraintDescriptor nodeLabelExistenceConstraint =
            ConstraintDescriptorFactory.nodeLabelExistenceForLabel(0, 1);
    private final ConstraintDescriptor nodeLabelExistenceAnotherConstrainedLabelConstraint =
            ConstraintDescriptorFactory.nodeLabelExistenceForLabel(2, 1);
    private final ConstraintDescriptor nodeLabelExistenceAnotherRequiredLabelConstraint =
            ConstraintDescriptorFactory.nodeLabelExistenceForLabel(0, 2);

    private final InMemoryTokens lookup = new InMemoryTokens()
            .label(0, "La:bel")
            .label(1, "Label1")
            .label(2, "Label2")
            .label(3, "Label3")
            .relationshipType(0, "Ty:pe")
            .relationshipType(1, "Type1")
            .relationshipType(2, "Type2")
            .relationshipType(3, "Type3")
            .propertyKey(0, "prop:erty")
            .propertyKey(1, "prop1")
            .propertyKey(2, "prop2")
            .propertyKey(3, "prop3");

    /**
     * There are many tests throughout the code base that end up relying on indexes getting specific names.
     * For that reason, we need to keep the hash function output relatively pinned down.
     */
    @Test
    void mustGenerateDeterministicNames() {
        assertName(rangeLabelPrototype, "index_d1102d81");
        assertName(rangeLabelUniquePrototype, "index_4bc572bd");
        assertName(rangeRelTypePrototype, "index_5375b6a1");
        assertName(rangeRelTypeUniquePrototype, "index_3d38ec3a");
        assertName(nodeFtsPrototype, "index_43bd4380");
        assertName(relFtsPrototype, "index_eda167c6");
        assertName(uniqueLabelConstraint, "constraint_d68af99b");
        assertName(uniqueRelTypeConstraint, "constraint_c289e715");
        assertName(existsLabelConstraint, "constraint_a21dea66");
        assertName(nodeKeyConstraint, "constraint_b902d068");
        assertName(relKeyConstraint, "constraint_3e0cfb8a");
        assertName(existsRelTypeConstraint, "constraint_e7fdd59c");
        assertName(nodeTypeConstraintIntBool, "constraint_b63f02cc");
        assertName(nodeTypeConstraintBoolInt, "constraint_b63f02cc");
        assertName(nodeTypeConstraintBoolString, "constraint_d62f8724");
        assertName(nodeTypeConstraintIntBool, "constraint_b63f02cc");
        assertName(nodeTypeConstraintBoolInt, "constraint_b63f02cc");
        assertName(relationshipEndpointLabelStartConstraint, "constraint_a91552f1");
        assertName(relationshipEndpointLabelStartAnotherLabelConstraint, "constraint_71750c54");
        assertName(relationshipEndpointLabelStartAnotherRelTypeConstraint, "constraint_1129c6f4");
        assertName(relationshipEndpointLabelEndConstraint, "constraint_8fede701");
        assertName(nodeLabelExistenceConstraint, "constraint_fbe5f296");
        assertName(nodeLabelExistenceAnotherConstrainedLabelConstraint, "constraint_4a716cb0");
        assertName(nodeLabelExistenceAnotherRequiredLabelConstraint, "constraint_baf79396");
        assertName(allLabelsPrototype, "index_460996c0");
        assertName(allRelTypesPrototype, "index_1b9dcc97");
        assertName(textLabelPrototype, "index_5671d826");
        assertName(textRelTypePrototype, "index_57d7e912");
        assertName(pointLabelPrototype, "index_91c2db3");
        assertName(pointRelTypePrototype, "index_48139637");
        assertName(vectorLabelPrototype, "index_e355fc1c");
        assertName(vectorRelTypePrototype, "index_b15dec2b");
        assertName(vectorLabelPrototype2, "index_677371f0");
    }

    @Test
    void mustGenerateReasonableUserDescription() {
        assertUserDescription(
                "Index( type='RANGE', schema=(:Label1 {prop2, prop3}), indexProvider='Undecided-0' )",
                rangeLabelPrototype);
        assertUserDescription(
                "Index( type='RANGE', schema=(:`La:bel` {`prop:erty`, prop1}), indexProvider='Undecided-0' )",
                rangeLabelPrototype2);
        assertUserDescription(
                "Index( type='RANGE', schema=(:Label1 {prop2, prop3}), indexProvider='Undecided-0' )",
                rangeLabelUniquePrototype);
        assertUserDescription(
                "Index( type='RANGE', schema=()-[:Type1 {prop2, prop3}]-(), indexProvider='Undecided-0' )",
                rangeRelTypePrototype);
        assertUserDescription(
                "Index( type='RANGE', schema=()-[:Type1 {prop2, prop3}]-(), indexProvider='Undecided-0' )",
                rangeRelTypeUniquePrototype);
        assertUserDescription(
                "Index( type='FULLTEXT', schema=(:Label1|Label2 {prop1, prop2}), indexProvider='Undecided-0' )",
                nodeFtsPrototype);
        assertUserDescription(
                "Index( type='FULLTEXT', schema=()-[:Type1|Type2 {prop1, prop2}]-(), indexProvider='Undecided-0' )",
                relFtsPrototype);
        assertUserDescription(
                "Index( type='LOOKUP', schema=(:<any-labels>), indexProvider='Undecided-0' )", allLabelsPrototype);
        assertUserDescription(
                "Index( type='LOOKUP', schema=()-[:<any-types>]-(), indexProvider='Undecided-0' )",
                allRelTypesPrototype);
        assertUserDescription(
                "Index( type='TEXT', schema=(:Label1 {prop2}), indexProvider='Undecided-0' )", textLabelPrototype);
        assertUserDescription(
                "Index( type='TEXT', schema=()-[:Type1 {prop2}]-(), indexProvider='Undecided-0' )",
                textRelTypePrototype);
        assertUserDescription(
                "Index( type='POINT', schema=(:Label1 {prop2}), indexProvider='Undecided-0' )", pointLabelPrototype);
        assertUserDescription(
                "Index( type='POINT', schema=()-[:Type1 {prop2}]-(), indexProvider='Undecided-0' )",
                pointRelTypePrototype);
        assertUserDescription(
                "Index( type='VECTOR', schema=(:Label1|Label2 {prop1, prop2}), indexProvider='Undecided-0' )",
                vectorLabelPrototype);
        assertUserDescription(
                "Index( type='VECTOR', schema=()-[:Type1|Type2 {prop1, prop2}]-(), indexProvider='Undecided-0' )",
                vectorRelTypePrototype);
        assertUserDescription(
                "Index( type='VECTOR', schema=(:`La:bel`|Label1 {`prop:erty`, prop1}), indexProvider='Undecided-0' )",
                vectorLabelPrototype2);
        assertUserDescription(
                "Constraint( type='NODE PROPERTY EXISTENCE', schema=(:Label1 {prop2, prop3}), graphTypeDependence='INDEPENDENT' )",
                existsLabelConstraint);
        assertUserDescription(
                "Constraint( type='RELATIONSHIP PROPERTY EXISTENCE', schema=()-[:Type1 {prop2, prop3}]-(), graphTypeDependence='INDEPENDENT' )",
                existsRelTypeConstraint);
        assertUserDescription(
                "Index( type='FULLTEXT', schema=(:`La:bel`|Label1 {`prop:erty`, prop1}), indexProvider='Undecided-0' )",
                nodeFtsPrototype2);
        assertUserDescription(
                "Constraint( type='NODE PROPERTY UNIQUENESS', schema=(:`La:bel` {`prop:erty`, prop1}), graphTypeDependence='UNDESIGNATED' )",
                uniqueLabelConstraint2);
        assertUserDescription(
                "Constraint( type='RELATIONSHIP PROPERTY UNIQUENESS', schema=()-[:Type1 {prop2, prop3}]-(), graphTypeDependence='UNDESIGNATED' )",
                uniqueRelTypeConstraint);
        assertUserDescription(
                "Constraint( type='NODE PROPERTY TYPE', schema=(:Label1 {prop2, prop3}), graphTypeDependence='INDEPENDENT', propertyType=BOOLEAN | INTEGER )",
                nodeTypeConstraintBoolInt);
        assertUserDescription(
                "Constraint( type='NODE PROPERTY TYPE', schema=(:Label1 {prop2, prop3}), graphTypeDependence='INDEPENDENT', propertyType=BOOLEAN | INTEGER )",
                nodeTypeConstraintIntBool);
        assertUserDescription(
                "Constraint( type='NODE PROPERTY TYPE', schema=(:Label1 {prop2, prop3}), graphTypeDependence='INDEPENDENT', propertyType=BOOLEAN | STRING )",
                nodeTypeConstraintBoolString);
        assertUserDescription(
                "Constraint( type='RELATIONSHIP PROPERTY TYPE', schema=()-[:Type1 {prop2, prop3}]-(), graphTypeDependence='INDEPENDENT', propertyType=BOOLEAN | STRING )",
                relationshipTypeConstraintBoolInt);
        assertUserDescription(
                "Constraint( type='RELATIONSHIP PROPERTY TYPE', schema=()-[:Type1 {prop2, prop3}]-(), graphTypeDependence='INDEPENDENT', propertyType=BOOLEAN | STRING )",
                relationshipTypeConstraintIntBool);
        assertUserDescription(
                "Index( name='rangeLabelPrototypeNamed', type='RANGE', schema=(:Label1 {prop2, prop3}), indexProvider='Undecided-0' )",
                rangeLabelPrototypeNamed);
        assertUserDescription(
                "Index( name='labelPrototype2Named', type='RANGE', schema=(:`La:bel` {`prop:erty`, prop1}), indexProvider='Undecided-0' )",
                rangeLabelPrototype2Named);
        assertUserDescription(
                "Index( name='rangeLabelUniquePrototypeNamed', type='RANGE', schema=(:Label1 {prop2, prop3}), indexProvider='Undecided-0' )",
                rangeLabelUniquePrototypeNamed);
        assertUserDescription(
                "Index( name='rangeRelTypePrototypeNamed', type='RANGE', schema=()-[:Type1 {prop2, prop3}]-(), indexProvider='Undecided-0' )",
                rangeRelTypePrototypeNamed);
        assertUserDescription(
                "Index( name='rangeRelTypeUniquePrototypeNamed', type='RANGE', schema=()-[:Type1 {prop2, prop3}]-(), indexProvider='Undecided-0' )",
                rangeRelTypeUniquePrototypeNamed);
        assertUserDescription(
                "Index( name='nodeFtsPrototypeNamed', type='FULLTEXT', schema=(:Label1|Label2 {prop1, prop2}), indexProvider='Undecided-0' )",
                nodeFtsPrototypeNamed);
        assertUserDescription(
                "Index( name='relFtsPrototypeNamed', type='FULLTEXT', schema=()-[:Type1|Type2 {prop1, prop2}]-(), indexProvider='Undecided-0' )",
                relFtsPrototypeNamed);
        assertUserDescription(
                "Index( name='nodeFtsPrototype2Named', type='FULLTEXT', schema=(:`La:bel`|Label1 {`prop:erty`, prop1}), indexProvider='Undecided-0' )",
                nodeFtsPrototype2Named);
        assertUserDescription(
                "Index( name='allLabelsPrototypeNamed', type='LOOKUP', schema=(:<any-labels>), indexProvider='Undecided-0' )",
                allLabelsPrototypeNamed);
        assertUserDescription(
                "Index( name='allRelTypesPrototypeNamed', type='LOOKUP', schema=()-[:<any-types>]-(), indexProvider='Undecided-0' )",
                allRelTypesPrototypeNamed);
        assertUserDescription(
                "Index( name='textLabelPrototypeNamed', type='TEXT', schema=(:Label1 {prop2}), indexProvider='Undecided-0' )",
                textLabelPrototypeNamed);
        assertUserDescription(
                "Index( name='textRelTypePrototypeNamed', type='TEXT', schema=()-[:Type1 {prop2}]-(), indexProvider='Undecided-0' )",
                textRelTypePrototypeNamed);
        assertUserDescription(
                "Index( name='pointLabelPrototypeNamed', type='POINT', schema=(:Label1 {prop2}), indexProvider='Undecided-0' )",
                pointLabelPrototypeNamed);
        assertUserDescription(
                "Index( name='pointRelTypePrototypeNamed', type='POINT', schema=()-[:Type1 {prop2}]-(), indexProvider='Undecided-0' )",
                pointRelTypePrototypeNamed);
        assertUserDescription(
                "Index( name='vectorLabelPrototypeNamed', type='VECTOR', schema=(:Label1|Label2 {prop1, prop2}), indexProvider='Undecided-0' )",
                vectorLabelPrototypeNamed);
        assertUserDescription(
                "Index( name='vectorLabelPrototype2Named', type='VECTOR', schema=(:`La:bel`|Label1 {`prop:erty`, prop1}), indexProvider='Undecided-0' )",
                vectorLabelPrototype2Named);
        assertUserDescription(
                "Index( name='vectorRelTypePrototypeNamed', type='VECTOR', schema=()-[:Type1|Type2 {prop1, prop2}]-(), indexProvider='Undecided-0' )",
                vectorRelTypePrototypeNamed);
        assertUserDescription(
                "Index( id=1, name='rangeLabelIndexNamed', type='RANGE', schema=(:Label1 {prop2, prop3}), indexProvider='Undecided-0' )",
                rangeLabelIndexNamed);
        assertUserDescription(
                "Index( id=2, name='labelIndex2Named', type='RANGE', schema=(:`La:bel` {`prop:erty`, prop1}), indexProvider='Undecided-0' )",
                rangeLabelIndex2Named);
        assertUserDescription(
                "Index( id=3, name='rangeLabelUniqueIndexNamed', type='RANGE', schema=(:Label1 {prop2, prop3}), indexProvider='Undecided-0' )",
                rangeLabelUniqueIndexNamed);
        assertUserDescription(
                "Index( id=4, name='rangeRelTypeIndexNamed', type='RANGE', schema=()-[:Type1 {prop2, prop3}]-(), indexProvider='Undecided-0' )",
                rangeRelTypeIndexNamed);
        assertUserDescription(
                "Index( id=5, name='rangeRelTypeUniqueIndexNamed', type='RANGE', schema=()-[:Type1 {prop2, prop3}]-(), indexProvider='Undecided-0' )",
                rangeRelTypeUniqueIndexNamed);
        assertUserDescription(
                "Index( id=6, name='nodeFtsIndexNamed', type='FULLTEXT', schema=(:Label1|Label2 {prop1, prop2}), indexProvider='Undecided-0' )",
                nodeFtsIndexNamed);
        assertUserDescription(
                "Index( id=7, name='relFtsIndexNamed', type='FULLTEXT', schema=()-[:Type1|Type2 {prop1, prop2}]-(), indexProvider='Undecided-0' )",
                relFtsIndexNamed);
        assertUserDescription(
                "Index( id=8, name='nodeFtsIndex2Named', type='FULLTEXT', schema=(:`La:bel`|Label1 {`prop:erty`, prop1}), "
                        + "indexProvider='Undecided-0' )",
                nodeFtsIndex2Named);
        assertUserDescription(
                "Index( id=9, name='allLabelsIndexNamed', type='LOOKUP', schema=(:<any-labels>), indexProvider='Undecided-0' )",
                allLabelsIndexNamed);
        assertUserDescription(
                "Index( id=10, name='allRelTypesIndexNamed', type='LOOKUP', schema=()-[:<any-types>]-(), indexProvider='Undecided-0' )",
                allRelTypesIndexNamed);
        assertUserDescription(
                "Index( id=11, name='textLabelIndexNamed', type='TEXT', schema=(:Label1 {prop2}), indexProvider='Undecided-0' )",
                textLabelIndexNamed);
        assertUserDescription(
                "Index( id=12, name='textRelTypeIndexNamed', type='TEXT', schema=()-[:Type1 {prop2}]-(), indexProvider='Undecided-0' )",
                textRelTypeIndexNamed);
        assertUserDescription(
                "Index( id=13, name='pointLabelIndexNamed', type='POINT', schema=(:Label1 {prop2}), indexProvider='Undecided-0' )",
                pointLabelIndexNamed);
        assertUserDescription(
                "Index( id=14, name='pointRelTypeIndexNamed', type='POINT', schema=()-[:Type1 {prop2}]-(), indexProvider='Undecided-0' )",
                pointRelTypeIndexNamed);
        assertUserDescription(
                "Index( id=15, name='indexBelongingToConstraint', type='RANGE', schema=(:Label1 {prop2, prop3}), "
                        + "indexProvider='Undecided-0', owningConstraint=1 )",
                indexBelongingToConstraint);
        assertUserDescription(
                "Index( id=16, name='vectorLabelIndexNamed', type='VECTOR', schema=(:Label1|Label2 {prop1, prop2}), "
                        + "indexProvider='Undecided-0' )",
                vectorLabelIndexNamed);
        assertUserDescription(
                "Index( id=17, name='vectorLabelIndex2Named', type='VECTOR', "
                        + "schema=(:`La:bel`|Label1 {`prop:erty`, prop1}), indexProvider='Undecided-0' )",
                vectorLabelIndex2Named);
        assertUserDescription(
                "Index( id=18, name='vectorRelTypeIndexNamed', type='VECTOR', "
                        + "schema=()-[:Type1|Type2 {prop1, prop2}]-(), indexProvider='Undecided-0' )",
                vectorRelTypeIndexNamed);
        assertUserDescription(
                "Constraint( id=1, name='uniqueLabelConstraintNamed', type='NODE PROPERTY UNIQUENESS', schema=(:Label1 {prop2, prop3}), ownedIndex=1, graphTypeDependence='UNDESIGNATED' )",
                uniqueLabelConstraintNamed);
        assertUserDescription(
                "Constraint( id=7, name='uniqueRelTypeConstraintNamed', type='RELATIONSHIP PROPERTY UNIQUENESS', schema=()-[:Type1 {prop2, prop3}]-(), ownedIndex=1, graphTypeDependence='UNDESIGNATED' )",
                uniqueRelTypeConstraintNamed);
        assertUserDescription(
                "Constraint( id=2, name='existsLabelConstraintNamed', type='NODE PROPERTY EXISTENCE', schema=(:Label1 {prop2, prop3}), graphTypeDependence='INDEPENDENT' )",
                existsLabelConstraintNamed);
        assertUserDescription(
                "Constraint( id=3, name='nodeKeyConstraintNamed', type='NODE KEY', schema=(:Label1 {prop2, prop3}), ownedIndex=3, graphTypeDependence='UNDESIGNATED' )",
                nodeKeyConstraintNamed);
        assertUserDescription(
                "Constraint( id=6, name='relKeyConstraintNamed', type='RELATIONSHIP KEY', schema=()-[:Type1 {prop2, prop3}]-(), ownedIndex=4, graphTypeDependence='UNDESIGNATED' )",
                relKeyConstraintNamed);
        assertUserDescription(
                "Constraint( id=4, name='existsRelTypeConstraintNamed', type='RELATIONSHIP PROPERTY EXISTENCE', schema=()-[:Type1 {prop2, prop3}]-(), graphTypeDependence='INDEPENDENT' )",
                existsRelTypeConstraintNamed);
        assertUserDescription(
                "Constraint( id=5, name='uniqueLabelConstraint2Named', type='NODE PROPERTY UNIQUENESS', schema=(:`La:bel` {`prop:erty`, prop1}), ownedIndex=5, graphTypeDependence='UNDESIGNATED' )",
                uniqueLabelConstraint2Named);
        assertUserDescription(
                "Constraint( id=10, name='namedNodeTypeConstraint', type='NODE PROPERTY TYPE', schema=(:Label1 {prop2, prop3}), graphTypeDependence='INDEPENDENT', propertyType=STRING )",
                namedNodeTypeConstraint);
        assertUserDescription(
                "Constraint( id=11, name='namedRelationshipTypeConstraint', type='RELATIONSHIP PROPERTY TYPE', schema=()-[:Type1 {prop2, prop3}]-(), graphTypeDependence='INDEPENDENT', propertyType=STRING )",
                namedRelationshipTypeConstraint);
    }

    private void assertName(SchemaDescriptorSupplier schemaish, String expectedName) {
        String generateName = SchemaNameUtil.generateName(schemaish, lookup);
        assertThat(generateName).isEqualTo(expectedName);
        assertThat(SchemaNameUtil.sanitiseName(generateName)).isEqualTo(expectedName);
    }

    private void assertUserDescription(String description, SchemaDescriptorSupplier schemaish) {
        assertThat(schemaish.userDescription(lookup))
                .as("wrong userDescription for " + schemaish)
                .isEqualTo(description);
    }

    @SuppressWarnings({"OptionalAssignedToNull", "ConstantConditions"})
    @Test
    void sanitiseNameMustRejectEmptyOptionalOrNullNames() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> SchemaNameUtil.sanitiseName(Optional.empty()));
        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(() -> SchemaNameUtil.sanitiseName((Optional<String>) null));
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> SchemaNameUtil.sanitiseName((String) null));
    }

    @Test
    void sanitiseNameMustRejectReservedNames() {
        Set<String> reservedNames = ReservedSchemaRuleNames.getReservedNames();
        reservedNames = reservedNames.stream()
                .flatMap(n -> Stream.of(" " + n, n, n + " "))
                .collect(Collectors.toSet());
        for (String reservedName : reservedNames) {
            assertThatExceptionOfType(IllegalArgumentException.class)
                    .as("reserved name: '" + reservedName + "'")
                    .isThrownBy(() -> SchemaNameUtil.sanitiseName(reservedName));
        }
    }

    @Test
    void sanitiseNameMustRejectInvalidNames() {
        List<String> invalidNames = List.of("", "\0", " ", "  ", "\t", " \t ", "\n", "\r");

        for (String invalidName : invalidNames) {
            assertThatExceptionOfType(IllegalArgumentException.class)
                    .as("invalid name: '" + invalidName + "'")
                    .isThrownBy(() -> SchemaNameUtil.sanitiseName(invalidName));
        }
    }

    @Test
    void sanitiseNameMustAcceptValidNames() {
        List<String> validNames = List.of(
                ".",
                ",",
                "'",
                "a",
                " a",
                "a ",
                "a b",
                "a\n",
                "a\nb",
                "\"",
                "@",
                "#",
                "$",
                "%",
                "{",
                "}",
                "\uD83D\uDE02",
                ":",
                ";",
                "[",
                "]",
                "-",
                "_",
                "`",
                "``",
                "`a`",
                "a`b",
                "a``b");

        for (String validName : validNames) {
            SchemaNameUtil.sanitiseName(validName);
        }
    }
}
