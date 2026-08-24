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
package org.neo4j.wal.entry;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Used to signal an unrecognised LogFormat version byte, either due to old software reading newer files,
 * or the files not being actual log files.
 * This exception is still an {@link IOException}, but a specific subclass of it as to make possible
 * special handling.
 */
public class UnknownLogFormatException extends IOException {
    public UnknownLogFormatException(Path file, byte versionByte) {
        super(template(file, versionByte));
    }

    private static String template(Path file, byte versionByte) {
        StringBuilder builder = new StringBuilder("Unable to read log version and last committed tx");
        if (file != null) {
            builder.append(" from '").append(file.toAbsolutePath()).append('\'');
        }
        builder.append(". The version byte ")
                .append(versionByte)
                .append(String.format(" (0x%02X) ", versionByte))
                .append(" is unrecognised");
        return builder.toString();
    }
}
