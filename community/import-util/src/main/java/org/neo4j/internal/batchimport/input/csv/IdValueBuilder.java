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
package org.neo4j.internal.batchimport.input.csv;

import java.util.ArrayList;
import java.util.List;
import org.neo4j.batchimport.api.input.Group;
import org.neo4j.internal.helpers.collection.Iterables;

/**
 * Contains logic around a single or multiple :ID columns, the combined value and also which parts are stored
 * as properties on the node.
 * <p>
 * An empty id column still contributes a (null) part, so that a part's position in the combined value is stable
 * regardless of which parts are empty. Such a part contributes nothing but the delimiter to the combined value.
 * An id where every part is empty is no id at all, i.e. {@link #isEmpty()}.
 */
public class IdValueBuilder {
    public static final char DELIMITER = '\u0007'; // BEL char
    private final boolean delimitIDs;
    private final List<Part> parts = new ArrayList<>();
    private final StringBuilder builder = new StringBuilder();
    private boolean hasNonNullParts;
    private Group group;

    public IdValueBuilder(boolean delimitIds) {
        this.delimitIDs = delimitIds;
    }

    public void clear() {
        parts.clear();
        hasNonNullParts = false;
        group = null;
    }

    public void part(Object value, Header.Entry entry) {
        if (group != null && !entry.group().equals(group)) {
            throw new IllegalStateException(
                    "Multiple ID columns for different groups:" + group + " and " + entry.group());
        }
        parts.add(new Part(entry.name(), value));
        if (value != null) {
            hasNonNullParts = true;
        }
        this.group = entry.group();
    }

    public Object value() {
        if (!hasNonNullParts) {
            return null;
        }
        if (parts.size() == 1) {
            return parts.getFirst().value;
        }
        builder.setLength(0);
        for (int i = 0; i < parts.size(); i++) {
            if (i > 0 && delimitIDs) {
                builder.append(DELIMITER);
            }
            Object value = parts.get(i).value;
            if (value != null) {
                builder.append(value);
            }
        }
        return builder.toString();
    }

    public Group group() {
        return group;
    }

    public Iterable<Part> idPropertyValues() {
        return Iterables.filter(parts, p -> p.name != null && p.value != null);
    }

    public boolean isEmpty() {
        return !hasNonNullParts;
    }

    public record Part(String name, Object value) {}
}
