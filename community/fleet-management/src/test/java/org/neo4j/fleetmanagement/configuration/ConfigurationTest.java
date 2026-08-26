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
package org.neo4j.fleetmanagement.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import java.beans.PropertyChangeEvent;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.fleetmanagement.actions.FleetAction;
import org.neo4j.fleetmanagement.communication.model.ConfigurationResponse;

class ConfigurationTest {

    @Test
    void updateConfigurationIfPresentFiresPendingActionsChange() {
        var configuration = new Configuration();
        var events = new ArrayList<PropertyChangeEvent>();
        configuration.addPropertyChangeListener(events::add);
        var actions = List.of(new FleetAction(
                "a1", FleetAction.Type.GENERATE_AND_UPLOAD_DIAGNOSTICS_REPORT, FleetAction.Category.SERVER, null));
        var response = new ConfigurationResponse();
        response.setPendingActions(actions);

        Configuration.updateConfigurationIfPresent(configuration, response);

        assertThat(events).singleElement().satisfies(event -> {
            assertThat(event.getPropertyName()).isEqualTo(Configuration.PENDING_ACTIONS_CHANGE);
            assertThat(event.getNewValue()).isEqualTo(actions);
        });
    }

    @Test
    void updateConfigurationIfPresentWithoutPendingActionsFiresNothing() {
        var configuration = new Configuration();
        var events = new ArrayList<PropertyChangeEvent>();
        configuration.addPropertyChangeListener(events::add);

        Configuration.updateConfigurationIfPresent(configuration, new ConfigurationResponse());
        Configuration.updateConfigurationIfPresent(configuration, null);

        assertThat(events).isEmpty();
    }

    @Test
    void taskTypeFromString() {
        assertThat(Configuration.TaskType.fromString("diagnostic_report"))
                .isEqualTo(Configuration.TaskType.DIAGNOSTIC_REPORT);
        assertThat(Configuration.TaskType.fromString("DIAGNOSTIC_REPORT"))
                .isEqualTo(Configuration.TaskType.DIAGNOSTIC_REPORT);
        assertThat(Configuration.TaskType.fromString("not-a-task")).isEqualTo(Configuration.TaskType.UNKNOWN);
    }
}
