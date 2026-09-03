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
package org.neo4j.server.queryapi.versioning;

/**
 * Represents the version of the query API.
 *
 * @param major The major version number
 * @param minor The minor version number
 */
public record QueryVersion(short major, short minor) {

    public QueryVersion {
        if (major < 0 || minor < 0) {
            throw new IllegalArgumentException("Major and minor version numbers must be between 0 and 32767");
        }
    }

    public QueryVersion(int major, int minor) {
        this((short) major, (short) minor);
    }

    @Override
    public String toString() {
        return String.format("%d.%d", this.major, this.minor);
    }
}
