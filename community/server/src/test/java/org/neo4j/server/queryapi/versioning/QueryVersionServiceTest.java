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

import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class QueryVersionServiceTest {

    @ParameterizedTest
    @MethodSource("versions")
    void shouldBuildWithVersions(List<String> expected, List<QueryVersion> versionList) {
        var builder = new QueryVersionService.Builder();

        versionList.forEach(builder::withVersion);

        var service = builder.build();

        assertEquals(expected, service.getVersions());
    }

    static Stream<Arguments> versions() {
        return Stream.of(
                Arguments.of(List.of("2.1", "1.0"), List.of(new QueryVersion(2, 1), new QueryVersion(1, 0))),
                Arguments.of(List.of("1.0", "2.5"), List.of(new QueryVersion(1, 0), new QueryVersion(2, 5))),
                Arguments.of(
                        List.of("1.0", "2.5", "5.6", "32767.32767"),
                        List.of(
                                new QueryVersion(1, 0),
                                new QueryVersion(2, 5),
                                new QueryVersion(5, 6),
                                new QueryVersion(32767, 32767))));
    }
}
