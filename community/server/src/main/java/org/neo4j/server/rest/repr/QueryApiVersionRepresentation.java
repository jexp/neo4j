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
package org.neo4j.server.rest.repr;

import java.util.List;

/**
 * Represents the available versions of the Query API.
 * <p/>
 * Those versions are used by clients to determine which major versions
 * of the Query API and their current minor version are supported by the server.
 */
public class QueryApiVersionRepresentation extends MappingRepresentation {
    private static final String QUERY_API_VERSION_KEY = "query_api_versions";

    private final ListRepresentation queryApiVersionsRepr;

    public QueryApiVersionRepresentation(ListRepresentation queryApiVersionsRepr) {
        super(QUERY_API_VERSION_KEY);
        this.queryApiVersionsRepr = queryApiVersionsRepr;
    }

    public static QueryApiVersionRepresentation fromVersions(List<String> queryApiVersions) {
        return new QueryApiVersionRepresentation(ListRepresentation.string(queryApiVersions));
    }

    @Override
    protected void serialize(MappingSerializer serializer) {
        serializer.putList(QUERY_API_VERSION_KEY, queryApiVersionsRepr);
    }
}
