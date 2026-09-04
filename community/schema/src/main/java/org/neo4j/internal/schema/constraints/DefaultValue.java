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
package org.neo4j.internal.schema.constraints;

import java.util.Objects;
import org.neo4j.util.Preconditions;
import org.neo4j.values.ValueGenerator;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

/**
 * Generator of default values for {@link TypeConstraintDescriptor}, either a constant or a generator.
 */
public interface DefaultValue {
    Value value();

    record Constant(Value value) implements DefaultValue {
        public Constant {
            Preconditions.checkArgument(value != null && value != Values.NO_VALUE, "Invalid constant value:" + value);
        }
    }

    class Generator implements DefaultValue {
        private final ValueGenerator valueGenerator;

        public Generator(ValueGenerator valueGenerator) {
            this.valueGenerator = valueGenerator;
        }

        /**
         * The id of the generator, representing (and mapping to) a known value function e.g. uuid()
         * @return a stable id for this generator used for persistence.
         */
        public int generatorId() {
            return valueGenerator.id();
        }

        @Override
        public Value value() {
            return valueGenerator.generate();
        }

        @Override
        public String toString() {
            return String.format("generator(%s)", valueGenerator);
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Generator generator)) return false;
            return generatorId() == generator.generatorId();
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(generatorId());
        }
    }
}
