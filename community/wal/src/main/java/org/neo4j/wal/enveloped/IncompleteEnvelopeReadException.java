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

/**
 * Used to signal an incomplete log envelope read.
 * Differs from {@link InvalidLogEnvelopeReadException} in that this could be a recoverable scenario, where it is
 * only the last entry of the last file that was not completely written to the log.
 * This exception is still an {@link IOException}, but a specific subclass of it as to make possible
 * special handling.
 */
public class IncompleteEnvelopeReadException extends IOException {
    public IncompleteEnvelopeReadException(String message) {
        super(message);
    }

    public IncompleteEnvelopeReadException(Throwable cause) {
        super(cause);
    }

    public IncompleteEnvelopeReadException(String message, Throwable e) {
        super(message, e);
    }
}
