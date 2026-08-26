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
package org.neo4j.fleetmanagement.communication.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import java.util.List;
import java.util.Map;
import org.neo4j.fleetmanagement.actions.FleetAction;

public class ConfigurationResponse {
    private List<MetricsDefinitionResponse> metrics;

    @JsonProperty("neo4j_config_key_globs")
    private List<String> neo4jConfigKeyGlobs;

    @JsonProperty("reporting_intervals")
    private Map<String, String> reportingIntervals;

    @JsonProperty("new_token")
    private String newToken;

    @JsonProperty("pending_migrations_to_aura")
    @JsonPropertyDescription("List of pending migrations to Aura")
    private List<MigrationToAura> pendingMigrationsToAura;

    @JsonProperty("pending_actions")
    @JsonPropertyDescription("List of pending actions")
    private List<FleetAction> pendingActions;

    public List<MetricsDefinitionResponse> getMetrics() {
        return metrics;
    }

    public void setMetrics(List<MetricsDefinitionResponse> metrics) {
        this.metrics = metrics;
    }

    public void setNeo4jConfigKeyGlobs(List<String> neo4jConfigKeyGlobs) {
        this.neo4jConfigKeyGlobs = neo4jConfigKeyGlobs;
    }

    public void setReportingIntervals(Map<String, String> reportingIntervals) {
        this.reportingIntervals = reportingIntervals;
    }

    public Map<String, String> getReportingIntervals() {
        return reportingIntervals;
    }

    public List<String> getNeo4jConfigKeyGlobs() {
        return neo4jConfigKeyGlobs;
    }

    public String newToken() {
        return newToken;
    }

    public void setNewToken(String newToken) {
        this.newToken = newToken;
    }

    public List<MigrationToAura> getPendingMigrationsToAura() {
        return pendingMigrationsToAura;
    }

    public void setPendingMigrationsToAura(List<MigrationToAura> pendingMigrationsToAura) {
        this.pendingMigrationsToAura = pendingMigrationsToAura;
    }

    public List<FleetAction> getPendingActions() {
        return pendingActions;
    }

    public void setPendingActions(List<FleetAction> pendingActions) {
        this.pendingActions = pendingActions;
    }
}
