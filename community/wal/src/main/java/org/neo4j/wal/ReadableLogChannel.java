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
package org.neo4j.wal;

import java.io.IOException;

public interface ReadableLogChannel extends ReadableLogPositionAwareChannel, VersionableLog {
    // reset channel to position and clear any underlying buffering
    void resetToPosition(long byteOffset) throws IOException;

    // Get position of the first entry of the channel. Please note that in some of the channel
    // implementations(envelopes) the first entry is not the same as the first available data in the particular channel.
    LogPosition firstEntryPosition() throws IOException;
}
