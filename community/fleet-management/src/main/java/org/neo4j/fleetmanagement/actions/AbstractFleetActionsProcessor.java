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

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.neo4j.fleetmanagement.configuration.Configuration;

public abstract class AbstractFleetActionsProcessor implements FleetActionsProcessor, PropertyChangeListener {
    @Override
    public void propertyChange(PropertyChangeEvent evt) {
        if (evt.getPropertyName() == null
                || !Objects.equals(evt.getPropertyName(), Configuration.PENDING_ACTIONS_CHANGE)) {
            return;
        }

        var value = evt.getNewValue();
        if (!(value instanceof List<?> pendingActions) || pendingActions.isEmpty()) {
            return;
        }

        var processableActions = new ArrayList<FleetAction>();
        for (var actionObj : pendingActions) {
            if (evt.getOldValue() == evt.getNewValue()
                    || !(actionObj instanceof FleetAction action)
                    || action.type == FleetAction.Type.NONE) {
                continue;
            }

            processableActions.add(action);
        }

        if (processableActions.isEmpty()) {
            return;
        }

        this.process(processableActions);
    }

    public abstract void process(List<FleetAction> actions);
}
