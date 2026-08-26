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
package org.neo4j.fleetmanagement.actions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.neo4j.fleetmanagement.diagnostics.GenerateAndUploadDiagnosticsReport.GenerateAndUploadReportActionPayload;

class FleetActionTest {
    // mirrors the mapper configuration in BaseService used to deserialize ConfigurationResponse
    private static final ObjectMapper MAPPER =
            new ObjectMapper().enable(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_USING_DEFAULT_VALUE);

    @Test
    void deserializesKnownEnumValues() throws JsonProcessingException {
        var json = "{\"id\":\"a1\",\"type\":\"GENERATE_AND_UPLOAD_DIAGNOSTICS_REPORT\",\"category\":\"CLUSTER\"}";

        var action = MAPPER.readValue(json, FleetAction.class);

        assertThat(action.id).isEqualTo("a1");
        assertThat(action.type).isEqualTo(FleetAction.Type.GENERATE_AND_UPLOAD_DIAGNOSTICS_REPORT);
        assertThat(action.category).isEqualTo(FleetAction.Category.CLUSTER);
    }

    @Test
    void deserializesUnknownEnumValuesAsDefaults() throws JsonProcessingException {
        var json = "{\"id\":\"a1\",\"type\":\"SOME_FUTURE_TYPE\",\"category\":\"SOME_FUTURE_CATEGORY\"}";

        var action = MAPPER.readValue(json, FleetAction.class);

        assertThat(action.type).isEqualTo(FleetAction.Type.NONE);
        assertThat(action.category).isEqualTo(FleetAction.Category.SERVER);
    }

    @Test
    void deserializePayloadReturnsNullForMissingPayload() throws JsonProcessingException {
        assertThat(new FleetAction("a1", FleetAction.Type.NONE, FleetAction.Category.SERVER, null)
                        .deserializePayload(GenerateAndUploadReportActionPayload.class))
                .isNull();
        assertThat(new FleetAction("a1", FleetAction.Type.NONE, FleetAction.Category.SERVER, "  ")
                        .deserializePayload(GenerateAndUploadReportActionPayload.class))
                .isNull();
    }

    @Test
    void deserializePayloadIgnoresUnknownFields() throws JsonProcessingException {
        var action = new FleetAction(
                "a1",
                FleetAction.Type.GENERATE_AND_UPLOAD_DIAGNOSTICS_REPORT,
                FleetAction.Category.SERVER,
                "{\"uploadUrl\":\"https://example.com/upload?sig=abc\",\"unknownField\":42}");

        var payload = action.deserializePayload(GenerateAndUploadReportActionPayload.class);

        assertThat(payload.uploadUrl()).isEqualTo("https://example.com/upload?sig=abc");
    }

    @Test
    void deserializePayloadThrowsOnMalformedJson() {
        var action = new FleetAction(
                "a1",
                FleetAction.Type.GENERATE_AND_UPLOAD_DIAGNOSTICS_REPORT,
                FleetAction.Category.SERVER,
                "{not json");

        assertThatExceptionOfType(JsonProcessingException.class)
                .isThrownBy(() -> action.deserializePayload(GenerateAndUploadReportActionPayload.class));
    }

    @Test
    void equalityIsBasedOnAllFields() {
        var action = new FleetAction(
                "a1", FleetAction.Type.GENERATE_AND_UPLOAD_DIAGNOSTICS_REPORT, FleetAction.Category.SERVER, "p");
        var identical = new FleetAction(
                "a1", FleetAction.Type.GENERATE_AND_UPLOAD_DIAGNOSTICS_REPORT, FleetAction.Category.SERVER, "p");
        var differentId = new FleetAction(
                "a2", FleetAction.Type.GENERATE_AND_UPLOAD_DIAGNOSTICS_REPORT, FleetAction.Category.SERVER, "p");
        var differentType = new FleetAction("a1", FleetAction.Type.NONE, FleetAction.Category.SERVER, "p");
        var differentCategory = new FleetAction(
                "a1", FleetAction.Type.GENERATE_AND_UPLOAD_DIAGNOSTICS_REPORT, FleetAction.Category.CLUSTER, "p");
        var differentPayload = new FleetAction(
                "a1", FleetAction.Type.GENERATE_AND_UPLOAD_DIAGNOSTICS_REPORT, FleetAction.Category.SERVER, "other");

        assertThat(action).isEqualTo(identical).hasSameHashCodeAs(identical);
        assertThat(action)
                .isNotEqualTo(differentId)
                .isNotEqualTo(differentType)
                .isNotEqualTo(differentCategory)
                .isNotEqualTo(differentPayload)
                .isNotEqualTo(null)
                .isNotEqualTo("a1");
    }
}
