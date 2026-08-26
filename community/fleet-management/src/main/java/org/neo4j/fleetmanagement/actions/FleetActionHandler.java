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

import org.neo4j.fleetmanagement.utils.Logger;
import org.neo4j.logging.Log;

public abstract class FleetActionHandler {
    protected final Log userLog;
    protected final Logger fleetManagerLog;

    public FleetActionHandler() {
        this.userLog = Logger.getNeo4jLogger();
        this.fleetManagerLog = Logger.getFleetManagerLogger();
    }

    public FleetActionResult handle(FleetAction action) {
        try {
            fleetManagerLog.debug("Executing action %s", action.type);
            var result = execute(action);
            fleetManagerLog.debug("Action %s executed", action.type);
            return result;
        } catch (Exception e) {
            userLog.error("Fleet manager action failed to execute " + action.type, e);
            throw e;
        } catch (Error e) {
            // Catch Error separately as it's not a subclass of Exception
            userLog.error("Fleet manager action failed to execute " + action.type + " with Error", e);
            throw e; // Re-throw Error as it indicates a serious problem
        }
    }

    protected abstract FleetActionResult execute(FleetAction action);
}
