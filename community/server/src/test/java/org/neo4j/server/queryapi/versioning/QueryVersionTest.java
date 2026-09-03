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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class QueryVersionTest {

    @ParameterizedTest
    @MethodSource("versions")
    void shouldSerializeVersion(String expected, QueryVersion version) {
        assertEquals(expected, version.toString());
    }

    @ParameterizedTest
    @MethodSource("invalidVersions")
    void shouldRejectInvalidMajorAndMinorVersions(int major, int minor) {
        assertThrows(IllegalArgumentException.class, () -> new QueryVersion(major, minor));
    }

    static Stream<Arguments> versions() {
        return Stream.of(
                Arguments.of("2.1", new QueryVersion(2, 1)),
                Arguments.of("2.0", new QueryVersion(2, 0)),
                Arguments.of("1.1", new QueryVersion(1, 1)),
                Arguments.of("1.0", new QueryVersion(1, 0)),
                Arguments.of("0.1", new QueryVersion(0, 1)),
                Arguments.of("10.101", new QueryVersion(10, 101)),
                Arguments.of("32767.32767", new QueryVersion(32767, 32767)));
    }

    static Stream<Arguments> invalidVersions() {
        return Stream.of(
                Arguments.of(-1, 0),
                Arguments.of(0, -1),
                Arguments.of(-1, -1),
                Arguments.of(32768, 0),
                Arguments.of(0, 32768),
                Arguments.of(32768, 32768));
    }
}
