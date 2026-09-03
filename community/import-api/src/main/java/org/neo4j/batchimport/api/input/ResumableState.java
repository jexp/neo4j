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

import java.io.IOException;
import org.neo4j.batchimport.api.input.ResumableStateData.ResumableStateDataBuilder;

public interface ResumableState {

    /**
     * Checkpoints the state, writing whatever it has to the builder. On return, everything
     * has been made durable in the underlying resource, to the extent that the resource supports it.
     */
    void checkpoint(ResumableStateDataBuilder resumableStateDataBuilder) throws IOException;

    /**
     * Restores the state previously written by {@link #checkpoint(ResumableStateDataBuilder)}.
     */
    void resumeFromCheckpoint(ResumableStateData resumableStateData) throws IOException;

    /**
     * Restores the state from before anything was checkpointed, discarding whatever an earlier attempt that never
     * reached a {@link #checkpoint(ResumableStateDataBuilder) checkpoint} did.
     */
    void resumeFromStart() throws IOException;
}
