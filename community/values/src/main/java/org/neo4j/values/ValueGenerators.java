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
package org.neo4j.values;

import java.util.UUID;
import org.eclipse.collections.api.factory.primitive.IntObjectMaps;
import org.eclipse.collections.api.map.primitive.IntObjectMap;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

/**
 * Library of known {@link ValueGenerator}.
 * TODO these value generators should probably be shared with or directly used by Cypher for its functions, such as uuid()
 */
public enum ValueGenerators implements ValueGenerator {
    uuid(0) {
        @Override
        public Value generate() {
            return Values.uuidValue(UUID.randomUUID());
        }
    };

    private final int id;

    ValueGenerators(int id) {
        this.id = id;
    }

    @Override
    public int id() {
        return id;
    }

    private static final IntObjectMap<ValueGenerator> GENERATORS;

    static {
        MutableIntObjectMap<ValueGenerator> generators = IntObjectMaps.mutable.empty();
        for (ValueGenerators generator : ValueGenerators.values()) {
            generators.put(generator.id(), generator);
        }
        GENERATORS = generators.toImmutable();
    }

    public static ValueGenerator byId(int id) {
        ValueGenerator generator = GENERATORS.get(id);
        if (generator == null) {
            throw new IllegalArgumentException("No generator by id " + id);
        }
        return generator;
    }
}
