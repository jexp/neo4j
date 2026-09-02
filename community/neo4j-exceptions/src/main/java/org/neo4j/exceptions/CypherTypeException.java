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
package org.neo4j.exceptions;

import static java.lang.String.format;

import java.util.List;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlHelper;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.kernel.api.exceptions.Status;

public class CypherTypeException extends Neo4jException {

    protected CypherTypeException(ErrorGqlStatusObject gqlStatusObject, String message, Throwable cause) {
        super(gqlStatusObject, message, cause);
    }

    protected CypherTypeException(ErrorGqlStatusObject gqlStatusObject, String message) {
        super(gqlStatusObject, message);
    }

    public static CypherTypeException internalError(String msgTitle, String message) {
        var gql = GqlHelper.get50N00(msgTitle, message);
        return new CypherTypeException(gql, message);
    }

    public static CypherTypeException invalidType(
            String value, List<String> expectedTypes, String actualType, String signature) {
        var gql = GqlHelper.getGql22G03_22N01(value, expectedTypes, actualType);
        return new CypherTypeException(gql, String.format("Wrong type. Expected %s, got %s", signature, actualType));
    }

    public static CypherTypeException invalidTypeForLabelExpression(
            String value, String prettyValue, String actualType, String actualCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(prettyValue, List.of("NODE", "RELATIONSHIP"), actualCypherType);
        return new CypherTypeException(
                gql,
                String.format(
                        "Invalid input for function 'hasALabelOrType()': Expected %s to be a node or relationship, but it was `%s`",
                        value, actualType));
    }

    public static CypherTypeException nodeCreationNotAMap(String value, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(value, List.of("MAP"), gotCypherType);
        return new CypherTypeException(
                gql, String.format("Parameter provided for node creation is not a Map, instead got %s", value));
    }

    public static CypherTypeException expectedMap(String got, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("MAP"), gotCypherType);
        return new CypherTypeException(gql, String.format("Type mismatch: expected a map but was %s", got));
    }

    public static CypherTypeException expectedExpressionToBeMap(
            String expression, String got, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("MAP"), gotCypherType);
        return new CypherTypeException(
                gql, String.format("Expected %s to be a map, but it was :`%s`", expression, got));
    }

    public static CypherTypeException expectedPathButGot(String gotPretty, String gotType, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("PATH"), gotCypherType);
        return new CypherTypeException(gql, String.format("Expected path but got %s", gotType));
    }

    public static CypherTypeException expectedNodeRelPath(String gotPretty, String gotClass, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("NODE", "RELATIONSHIP", "PATH"), gotCypherType);
        return new CypherTypeException(
                gql, String.format("Expected a Node, Relationship or Path, but got a %s", gotClass));
    }

    public static CypherTypeException expectedNodeRelWas(
            String got, String gotClass, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("NODE", "RELATIONSHIP"), gotCypherType);
        return new CypherTypeException(
                gql, String.format("Expected %s to be a Node or Relationship, but it was a %s", got, gotClass));
    }

    public static CypherTypeException expectedNodeRel(String gotClass, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("NODE", "RELATIONSHIP"), gotCypherType);
        return new CypherTypeException(gql, String.format("Expected a Node or Relationship, but got a %s", gotClass));
    }

    public static CypherTypeException expectedListValue(String got, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("LIST"), gotCypherType);
        return new CypherTypeException(gql, String.format("Expected ListValue but got %s", got));
    }

    public static CypherTypeException expectedList(String got, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("LIST"), gotCypherType);
        return new CypherTypeException(gql, String.format("Expected list, got %s", got));
    }

    public static CypherTypeException expectedListFound(String got, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("LIST"), gotCypherType);
        return new CypherTypeException(gql, String.format("Expected list but found: %s", got));
    }

    public static CypherTypeException expectedCollection(String got, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("LIST"), gotCypherType);
        return new CypherTypeException(gql, String.format("Expected a collection, got `%s`", got));
    }

    public static CypherTypeException expectedCollectionWasNot(String got, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("LIST"), gotCypherType);
        return new CypherTypeException(
                gql, String.format("Expected the value for %s to be a collection but it was not.", got));
    }

    public static CypherTypeException planExpectedNode(String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("NODE"), gotCypherType);
        return new CypherTypeException(gql, "Created a plan that uses non-nodes when expecting a node");
    }

    public static CypherTypeException notBool(String got, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("BOOLEAN"), gotCypherType);
        return new CypherTypeException(gql, String.format("%s is not a boolean value", got));
    }

    public static CypherTypeException cantTreatAsBool(String got, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("BOOLEAN"), gotCypherType);
        return new CypherTypeException(gql, String.format("Don't know how to treat that as a boolean: %s", got));
    }

    public static CypherTypeException notNode(String got, String gotType) {
        var gql = GqlHelper.getGql22G03_22N01(got, List.of("NODE"), gotType);
        return new CypherTypeException(gql, String.format("%s is not a node", got));
    }

    public static CypherTypeException expectedString(String msg, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("STRING"), gotCypherType);
        return new CypherTypeException(gql, msg);
    }

    public static CypherTypeException expectedStringNotNull(String msg, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("STRING NOT NULL"), gotCypherType);
        return new CypherTypeException(gql, msg);
    }

    public static CypherTypeException expectedStringOrListOfStringsNotNull(
            String msg, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(
                gotPretty, List.of("STRING NOT NULL", "LIST<STRING NOT NULL>"), gotCypherType);
        return new CypherTypeException(gql, msg);
    }

    public static CypherTypeException expectedListOfNumber(String msg, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(
                gotPretty, List.of("LIST<INTEGER NOT NULL>", "LIST<FLOAT NOT NULL>"), gotCypherType);
        return new CypherTypeException(gql, msg);
    }

    public static CypherTypeException expectedNumber(String msg, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("INTEGER", "FLOAT"), gotCypherType);
        return new CypherTypeException(gql, msg);
    }

    public static CypherTypeException expectedInteger(String msg, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("INTEGER"), gotCypherType);
        return new CypherTypeException(gql, msg);
    }

    public static CypherTypeException functionArgumentWrongType(
            String msg, String functionName, String gotPretty, List<String> expectedList, String gotCypherType) {
        var gql = GqlHelper.getGql22N38_22N01(
                GqlParams.StringParam.fun.process(functionName), gotPretty, expectedList, gotCypherType);
        return new CypherTypeException(gql, msg);
    }

    public static CypherTypeException expectedNumericGotNull(String target) {
        var gql = GqlHelper.getGql22G03_22N01("NULL", List.of("INTEGER", "FLOAT"), "NULL");
        return new CypherTypeException(gql, String.format("Expected a numeric value for %s, but got null", target));
    }

    public static CypherTypeException expectedNumericGot(
            String target, String got, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("INTEGER", "FLOAT"), gotCypherType);
        return new CypherTypeException(gql, String.format("Expected a numeric value for %s, but got: %s", target, got));
    }

    public static CypherTypeException expectedVirtualNode(String gotPretty, String gotTypeName, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("NODE"), gotCypherType);
        return new CypherTypeException(gql, String.format("Expected VirtualNodeValue got %s", gotTypeName));
    }

    public static CypherTypeException expectedVirtualRelationship(
            String gotPretty, String gotTypeName, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("RELATIONSHIP"), gotCypherType);
        return new CypherTypeException(gql, String.format("Expected VirtualRelationshipValue got %s", gotTypeName));
    }

    public static CypherTypeException expectedNodeValue(String gotPretty, String gotTypeName, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("NODE"), gotCypherType);
        return new CypherTypeException(gql, String.format("Expected NodeValue but got %s", gotTypeName));
    }

    public static CypherTypeException typeMismatchExpectedANode(String got, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("NODE"), gotCypherType);
        return new CypherTypeException(gql, String.format("Type mismatch: expected a node but was %s", got));
    }

    public static CypherTypeException typeMismatchExpectedANodeWasType(
            String got, String gotClass, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("NODE"), gotCypherType);
        return new CypherTypeException(
                gql, String.format("Type mismatch: expected a node but was %s of type %s", got, gotClass));
    }

    public static CypherTypeException expectedNode(String got, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("NODE"), gotCypherType);
        return new CypherTypeException(gql, String.format("Expected a Node, got: %s", got));
    }

    public static CypherTypeException expectedNodeButGot(String gotPretty, String gotType, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("NODE"), gotCypherType);
        return new CypherTypeException(gql, String.format("Expected node but got %s", gotType));
    }

    public static CypherTypeException expectedANodeButGot(String gotPretty, String gotType, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("NODE"), gotCypherType);
        return new CypherTypeException(gql, String.format("Expected a node, but got %s ", gotType));
    }

    public static CypherTypeException expectedNodeButGotNull() {
        var gql = GqlHelper.getGql22G03_22N01("NULL", List.of("NODE"), "NULL");
        return new CypherTypeException(gql, "Expected a node, but got null ");
    }

    public static CypherTypeException expectedRelButGot(String gotPretty, String gotType, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("RELATIONSHIP"), gotCypherType);
        return new CypherTypeException(gql, String.format("Expected relationship but got %s", gotType));
    }

    public static CypherTypeException typeMismatchExpectedARel(String got, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("RELATIONSHIP"), gotCypherType);
        return new CypherTypeException(gql, String.format("Type mismatch: expected a relationship but was %s", got));
    }

    public static CypherTypeException expectedRel(String got, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("RELATIONSHIP"), gotCypherType);
        return new CypherTypeException(gql, String.format("Expected a Relationship, got: %s", got));
    }

    public static CypherTypeException expectedOtherType(
            String got, String expectedType, String gotType, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of(expectedType), gotCypherType);
        return new CypherTypeException(
                gql, String.format("Expected %s to be a %s, but it was a %s", got, expectedType, gotType));
    }

    public static CypherTypeException wrongVectorDimension(
            String gotPretty, String nestedTypeName, long expectedDimension, int gotDimension) {
        var expectedType = String.format("VECTOR<%s>(%d)", nestedTypeName, expectedDimension);
        var gotCypherType = String.format("VECTOR<%s>(%d)", nestedTypeName, gotDimension);
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of(expectedType), gotCypherType);
        return new CypherTypeException(gql, String.format("Expected a %s, but got %s", expectedType, gotCypherType));
    }

    public static CypherTypeException wrongVectorDimension(String indexName, int indexDim, int vectorDim) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N65)
                .withParam(GqlParams.StringParam.idx, indexName)
                .withParam(GqlParams.NumberParam.dim1, indexDim)
                .withParam(GqlParams.NumberParam.dim2, vectorDim)
                .build();

        return new CypherTypeException(
                gql,
                "Vector index '%s' has a configured dimensionality of %d, but the provided vector has dimension %d."
                        .formatted(indexName, indexDim, vectorDim));
    }

    public static CypherTypeException howTreatPredicate(String got, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("BOOLEAN"), gotCypherType);
        return new CypherTypeException(gql, String.format("Don't know how to treat a predicate: %s", got), null);
    }

    public static CypherTypeException howTreatAsPredicate(String got, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("BOOLEAN"), gotCypherType);
        return new CypherTypeException(gql, String.format("Don't know how to treat that as a predicate: %s", got));
    }

    public static CypherTypeException expectedPrimitivePropertyValue(
            String got, String gotPretty, String gotCypherType, Boolean withEncountered) {
        var gql = GqlHelper.getGql22G03_22N01(
                gotPretty,
                List.of(
                        "BOOLEAN",
                        "STRING",
                        "INTEGER",
                        "FLOAT",
                        "DATE",
                        "LOCAL TIME",
                        "ZONED TIME",
                        "LOCAL DATETIME",
                        "ZONED DATETIME",
                        "DURATION",
                        "POINT"),
                gotCypherType);
        String msg = "Property values can only be of primitive types or arrays thereof";
        if (withEncountered) msg += String.format(". Encountered: %s.", got);
        return new CypherTypeException(gql, msg);
    }

    public static CypherTypeException expectedRelValue(String got, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("RELATIONSHIP"), gotCypherType);
        return new CypherTypeException(gql, String.format("Expected %s to be a RelationshipValue", got));
    }

    public static CypherTypeException expectedRelValueGotType(String gotType, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("RELATIONSHIP"), gotCypherType);
        return new CypherTypeException(gql, String.format("Expected RelationshipValue but got %s", gotType));
    }

    public static CypherTypeException notCollectionOrMap(
            String got, String gotPretty, String gotCypherType, Object index) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("LIST", "MAP"), gotCypherType);
        return new CypherTypeException(
                gql,
                String.format(
                        "`%s` is not a collection or a map. Element access is only possible by performing a collection "
                                + "lookup using an integer index, or by performing a map lookup using a string key (found: %s[%s])",
                        got, got, index));
    }

    public static CypherTypeException notMap(String got, String gotPretty, String gotCypherType, Object index) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("MAP"), gotCypherType);
        return new CypherTypeException(
                gql,
                String.format(
                        "`%s` is not a map. Element access is only possible by performing a collection "
                                + "lookup by performing a map lookup using a string key (found: %s[%s])",
                        got, got, index));
    }

    public static CypherTypeException nonIntegerListIndex(String got, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("INTEGER"), gotCypherType);
        return new CypherTypeException(
                gql, String.format("Cannot access a list using an non-integer number index, got %s", got), null);
    }

    public static CypherTypeException addTypeMismatch(
            String rightPretty,
            String legacyLeftTypeName,
            String legacyRightTypeName,
            String rightCypherType,
            List<String> expectedRightCypherTypes) {
        var gql = GqlHelper.getGql22G03_22N01(rightPretty, expectedRightCypherTypes, rightCypherType);
        return new CypherTypeException(
                gql, String.format("Cannot add `%s` and `%s`", legacyLeftTypeName, legacyRightTypeName));
    }

    public static CypherTypeException subtractTypeMismatch(
            String rightPretty,
            String legacyLeftTypeName,
            String legacyRightTypeName,
            String rightCypherType,
            String expectedRightCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(rightPretty, List.of(expectedRightCypherType), rightCypherType);
        return new CypherTypeException(
                gql, String.format("Cannot subtract `%s` from `%s`", legacyRightTypeName, legacyLeftTypeName));
    }

    public static CypherTypeException divideTypeMismatch(
            String rightPretty,
            String legacyLeftTypeName,
            String legacyRightTypeName,
            String rightCypherType,
            List<String> expectedRightCypherTypes) {
        var gql = GqlHelper.getGql22G03_22N01(rightPretty, expectedRightCypherTypes, rightCypherType);
        return new CypherTypeException(
                gql, String.format("Cannot divide `%s` by `%s`", legacyLeftTypeName, legacyRightTypeName));
    }

    public static CypherTypeException multiplyTypeMismatch(
            String rightPretty,
            String legacyLeftTypeName,
            String legacyRightTypeName,
            String rightCypherType,
            List<String> expectedRightCypherTypes) {
        var gql = GqlHelper.getGql22G03_22N01(rightPretty, expectedRightCypherTypes, rightCypherType);
        return new CypherTypeException(
                gql, String.format("Cannot multiply `%s` and `%s`", legacyLeftTypeName, legacyRightTypeName));
    }

    public static CypherTypeException modulusTypeMismatch(
            String rightPretty,
            String legacyLeftTypeName,
            String legacyRightTypeName,
            String rightCypherType,
            List<String> expectedRightCypherTypes) {
        var gql = GqlHelper.getGql22G03_22N01(rightPretty, expectedRightCypherTypes, rightCypherType);
        return new CypherTypeException(
                gql,
                String.format("Cannot calculate modulus of `%s` and `%s`", legacyLeftTypeName, legacyRightTypeName));
    }

    public static CypherTypeException powerTypeMismatch(
            String rightPretty,
            String legacyLeftTypeName,
            String legacyRightTypeName,
            String rightCypherType,
            List<String> expectedRightCypherTypes) {
        var gql = GqlHelper.getGql22G03_22N01(rightPretty, expectedRightCypherTypes, rightCypherType);
        return new CypherTypeException(
                gql, String.format("Cannot raise `%s` to the power of `%s`", legacyLeftTypeName, legacyRightTypeName));
    }

    public static CypherTypeException concatenationTypeMismatch(
            String rightPretty,
            String legacyLeftTypeName,
            String legacyRightTypeName,
            String rightCypherType,
            String expectedRightCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(rightPretty, List.of(expectedRightCypherType), rightCypherType);
        return new CypherTypeException(
                gql, String.format("Cannot concatenate `%s` and `%s`", legacyLeftTypeName, legacyRightTypeName));
    }

    public static CypherTypeException propertyParamIsNotMap(String got, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("MAP"), gotCypherType);
        return new CypherTypeException(
                gql, String.format("Parameter provided for setting properties is not a Map, instead got %s", got));
    }

    public static CypherTypeException propertyWithRelCollection(List<?> collection) {
        var gql = GqlHelper.getGql22G03_22N39(String.valueOf(collection));
        return new CypherTypeException(
                gql, "Collections containing relationship values can not be stored in properties.");
    }

    public static CypherTypeException propertyWithNullInCollection(String serializedList) {
        var gql = GqlHelper.getGql22G03_22N39(serializedList);
        return new CypherTypeException(gql, "Collections containing null values can not be stored in properties.");
    }

    public static CypherTypeException propertyWithCollectionInCollection(String serializedList) {
        var gql = GqlHelper.getGql22G03_22N39(serializedList);
        return new CypherTypeException(gql, "Collections containing collections can not be stored in properties.");
    }

    public static CypherTypeException genericPropertyError(String value) {
        var gql = GqlHelper.getGql22G03_22N39(value);
        return new CypherTypeException(
                gql,
                "Neo4j only supports a subset of Cypher types for storage as singleton or array properties. "
                        + "Please refer to section cypher/syntax/values of the manual for more details.");
    }

    public static CypherTypeException collectionDifferentCRSPoints(String collection) {
        var gql = GqlHelper.getGql22G03_22N39(String.valueOf(collection));
        return new CypherTypeException(
                gql, "Collections containing point values with different CRS can not be stored in properties.");
    }

    public static CypherTypeException collectionDifferentDimPoints(String collection) {
        var gql = GqlHelper.getGql22G03_22N39(String.valueOf(collection));
        return new CypherTypeException(
                gql, "Collections containing point values with different dimensions can not be stored in properties.");
    }

    public static CypherTypeException expectedNodeAtRow(String row, String got) {
        var gql = GqlHelper.getGql22G03_22N27(row, got, List.of("NODE"));
        return new CypherTypeException(gql, String.format("Expected a node at `%s` but got %s", row, got));
    }

    public static CypherTypeException onlyNumericalValuesOrNullAllowed(
            String function, String value, String actualType, String actualCypherType) {
        var gql = GqlHelper.getGql22N38_22N01(function, value, List.of("INTEGER", "FLOAT"), actualCypherType);
        return new CypherTypeException(
                gql,
                String.format("%s can only handle numerical values or null, but received: %s", function, actualType));
    }

    public static CypherTypeException onlyNumericalValuesDurationsOrNullAllowed(
            String function, String value, String actualType, String actualCypherType) {
        var gql =
                GqlHelper.getGql22N38_22N01(function, value, List.of("INTEGER", "FLOAT", "DURATION"), actualCypherType);
        return new CypherTypeException(
                gql,
                String.format(
                        "%s can only handle numerical values, duration, or null, but received: %s",
                        function, actualType));
    }

    public static CypherTypeException onlyNumericalValuesAllowed(
            String function, String value, String actualCypherType) {
        var gql = GqlHelper.getGql22N38_22N01(function, value, List.of("INTEGER", "FLOAT"), actualCypherType);
        return new CypherTypeException(gql, String.format("%s cannot mix number and duration", function));
    }

    public static CypherTypeException onlyDurationValuesAllowed(
            String function, String value, String actualCypherType) {
        var gql = GqlHelper.getGql22N38_22N01(function, value, List.of("DURATION"), actualCypherType);
        return new CypherTypeException(gql, String.format("%s cannot mix number and duration", function));
    }

    public static CypherTypeException onlyDurationValuesAllowedButNumberFound(String function) {
        var gql = GqlHelper.getGql22N38_22NB1(function, List.of("DURATION"), "NUMERIC");
        return new CypherTypeException(gql, String.format("%s cannot mix number and duration", function));
    }

    public static CypherTypeException onlyNumberValuesAllowedButDurationFound(String function) {
        var gql = GqlHelper.getGql22N38_22NB1(function, List.of("NUMERIC"), "DURATION");
        return new CypherTypeException(gql, String.format("%s cannot mix number and duration", function));
    }

    public static CypherTypeException invalidCoercion(String value, String expectedType, String legacyMessage) {
        return new CypherTypeException(
                ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22G03)
                        .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N37)
                                .withParam(GqlParams.StringParam.value, value)
                                .withParam(GqlParams.StringParam.valueType, expectedType)
                                .build())
                        .build(),
                legacyMessage);
    }

    public static CypherTypeException integerOutOfBounds(String component, Number lower, Number upper, String input) {
        var gql = GqlHelper.getGql22003_22N03(component, "INTEGER", lower, upper, input);
        return new CypherTypeException(gql, format("integer, %s, is too large", input));
    }

    @Override
    public Status status() {
        return Status.Statement.TypeError;
    }
}
