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

import com.fasterxml.jackson.annotation.JsonEnumDefaultValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Objects;

public class FleetAction {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public String id;
    public Type type;
    public Category category;
    public String payload;

    public FleetAction() {}

    public FleetAction(String id, Type type, Category category, String payload) {
        this.id = id;
        this.type = type;
        this.category = category;
        this.payload = payload;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, type, payload, category);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        FleetAction that = (FleetAction) o;
        return Objects.equals(id, that.id)
                && type == that.type
                && Objects.equals(payload, that.payload)
                && category == that.category;
    }

    public enum Type {
        GENERATE_AND_UPLOAD_DIAGNOSTICS_REPORT,
        @JsonEnumDefaultValue
        NONE;
    }

    public enum Category {
        CLUSTER,
        @JsonEnumDefaultValue
        SERVER,
    }

    public <T> T deserializePayload(Class<T> payloadType) throws JsonProcessingException {
        if (payload == null || payload.isBlank()) {
            return null;
        }
        return OBJECT_MAPPER.readValue(payload, payloadType);
    }
}
