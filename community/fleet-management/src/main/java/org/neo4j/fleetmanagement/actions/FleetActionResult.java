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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Objects;

public class FleetActionResult {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public String id;
    public Status status;
    public String error;
    public String output;

    public FleetActionResult() {}

    public FleetActionResult(String id, Status status, String error, String output) {
        this.id = id;
        this.status = status;
        this.error = error;
        this.output = output;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        FleetActionResult that = (FleetActionResult) o;
        return Objects.equals(id, that.id)
                && Objects.equals(status, that.status)
                && Objects.equals(error, that.error)
                && Objects.equals(output, that.output);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, status, error, output);
    }

    @Override
    public String toString() {
        return "FleetActionResult{" + "id='" + id + '\'' + ", status='"
                + status + '\'' + ", error='"
                + error + '\'' + ", logs='"
                + output + '\'' + '}';
    }

    public <T> void serializeOutput(T output) throws JsonProcessingException {
        this.output = OBJECT_MAPPER.writeValueAsString(output);
    }

    public <T> void serializeError(T error) throws JsonProcessingException {
        this.error = OBJECT_MAPPER.writeValueAsString(error);
    }

    public enum Status {
        success,
        failed,
        running;
    }
}
