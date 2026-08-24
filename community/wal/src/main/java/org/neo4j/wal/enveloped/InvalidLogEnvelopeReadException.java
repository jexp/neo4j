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
package org.neo4j.wal.enveloped;

import java.io.IOException;
import org.neo4j.wal.entry.LogEnvelopeHeader.EnvelopeType;

/**
 * Used to signal an invalid log envelope read, e.g. expecting to read data in a zero-padded region.
 * This should only be used in non-recoverable scenarios, i.e. not when it is just the last write being incomplete.
 * This exception is still an {@link IOException}, but a specific subclass of it as to make possible
 * special handling.
 */
public class InvalidLogEnvelopeReadException extends IOException {
    public InvalidLogEnvelopeReadException(EnvelopeType unexpectedType, long segment, int position) {
        this(message("unexpected chunk type '%s' at position %d of segment %d"
                .formatted(unexpectedType, position, segment)));
    }

    public InvalidLogEnvelopeReadException(String message) {
        super(message);
    }

    private static String message(String message) {
        return "Unable to read log envelope data: " + message;
    }
}
