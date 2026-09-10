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
package org.neo4j.batchimport.api.input;

/**
 * Relationship IR reading/writing occurs in 2 phases:
 * <ul>
 *     <li>INITIAL_PASS - this is the pass that reads the IR and processes it based on the lowest node ID of that
 *     relationship, kicking any relationships into a higher range if required.
 *     ex. (1)-[2]->(101) would push this relationship into a higher range if there were 100 nodes or less in a
 *     range - see {@code NodeIdRange}</li>
 *     <li>FINAL_PASS - this is the pass that picks up any kicked forward relationships for processing based on the
 *     highest node ID of that relationship</li>
 * </ul>
 */
public enum RelationshipsIRPhase {
    INITIAL_PASS,
    FINAL_PASS
}
