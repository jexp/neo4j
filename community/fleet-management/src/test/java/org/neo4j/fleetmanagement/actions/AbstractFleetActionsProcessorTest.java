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

import java.beans.PropertyChangeEvent;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.fleetmanagement.configuration.Configuration;

class AbstractFleetActionsProcessorTest {
    private static final Object SOURCE = new Object();

    private final CapturingProcessor processor = new CapturingProcessor();

    private static FleetAction action(String id) {
        return new FleetAction(
                id, FleetAction.Type.GENERATE_AND_UPLOAD_DIAGNOSTICS_REPORT, FleetAction.Category.SERVER, null);
    }

    private void fire(Object oldValue, Object newValue) {
        processor.propertyChange(
                new PropertyChangeEvent(SOURCE, Configuration.PENDING_ACTIONS_CHANGE, oldValue, newValue));
    }

    @Test
    void processesPendingActionsChange() {
        var actions = List.of(action("a1"), action("a2"));

        fire(null, actions);

        assertThat(processor.processed).containsExactly(actions);
    }

    @Test
    void ignoresUnrelatedPropertyChanges() {
        processor.propertyChange(new PropertyChangeEvent(SOURCE, "metrics", null, List.of(action("a1"))));
        processor.propertyChange(new PropertyChangeEvent(SOURCE, null, null, List.of(action("a1"))));

        assertThat(processor.processed).isEmpty();
    }

    @Test
    void ignoresMissingOrEmptyNewValue() {
        fire(null, null);
        fire(null, "not-a-list");
        fire(null, List.of());

        assertThat(processor.processed).isEmpty();
    }

    @Test
    void filtersOutNoneTypeAndNonActionEntries() {
        var valid = action("a1");
        var noneType = new FleetAction("a2", FleetAction.Type.NONE, FleetAction.Category.SERVER, null);

        fire(null, List.of(noneType, "not-an-action"));
        assertThat(processor.processed).isEmpty();

        fire(null, List.of(valid, noneType, "not-an-action"));
        assertThat(processor.processed).containsExactly(List.of(valid));
    }

    @Test
    void ignoresEventWhereOldAndNewAreTheSameInstance() {
        var actions = List.of(action("a1"));

        fire(actions, actions);

        assertThat(processor.processed).isEmpty();
    }

    @Test
    void triggersOnConfigurationSetPendingActions() {
        var configuration = new Configuration();
        configuration.addPropertyChangeListener(processor);
        var actions = List.of(action("a1"));

        configuration.setPendingActions(actions);

        assertThat(processor.processed).containsExactly(actions);
    }

    private static class CapturingProcessor extends AbstractFleetActionsProcessor {
        final List<List<FleetAction>> processed = new ArrayList<>();

        @Override
        public void process(List<FleetAction> actions) {
            processed.add(actions);
        }
    }
}
