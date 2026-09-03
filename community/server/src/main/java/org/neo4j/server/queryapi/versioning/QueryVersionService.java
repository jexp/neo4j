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

import java.util.ArrayList;
import java.util.List;

/**
 * Provides the available versions of the Query API.
 * <p/>
 * This service provides the full list of available versions.
 */
public class QueryVersionService {

    private final List<String> versions;

    private QueryVersionService(List<QueryVersion> versions) {
        this.versions = versions.stream().map(QueryVersion::toString).toList();
    }

    public List<String> getVersions() {
        return this.versions;
    }

    public static class Builder {
        private final List<QueryVersion> versions;

        public Builder() {
            this.versions = new ArrayList<>();
        }

        public Builder withVersion(QueryVersion version) {
            this.versions.add(version);
            return this;
        }

        public QueryVersionService build() {
            return new QueryVersionService(this.versions);
        }
    }
}
