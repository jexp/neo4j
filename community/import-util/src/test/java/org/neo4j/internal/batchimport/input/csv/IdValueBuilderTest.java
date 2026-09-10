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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.internal.batchimport.input.csv.IdValueBuilder.DELIMITER;

import org.junit.jupiter.api.Test;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.internal.batchimport.input.Groups;

class IdValueBuilderTest {
    private final Groups groups = new Groups();
    private final Extractors extractors = new Extractors();

    @Test
    void shouldBeEmptyWithoutParts() {
        var builder = new IdValueBuilder(true);

        assertThat(builder.isEmpty()).isTrue();
        assertThat(builder.value()).isNull();
        assertThat(builder.group()).isNull();
        assertThat(builder.idPropertyValues()).isEmpty();
    }

    @Test
    void shouldKeepSinglePartAsItsOwnValue() {
        var builder = new IdValueBuilder(true);

        builder.part(123L, idEntry("localId", null));

        assertThat(builder.isEmpty()).isFalse();
        assertThat(builder.value()).isEqualTo(123L);
    }

    @Test
    void shouldCombinePartsWithDelimiter() {
        var builder = new IdValueBuilder(true);

        builder.part("US", idEntry("country", null));
        builder.part("1", idEntry("localId", null));

        assertThat(builder.value()).isEqualTo("US" + DELIMITER + "1");
    }

    @Test
    void shouldCombinePartsWithoutDelimiter() {
        var builder = new IdValueBuilder(false);

        builder.part("US", idEntry("country", null));
        builder.part("1", idEntry("localId", null));

        assertThat(builder.value()).isEqualTo("US1");
    }

    @Test
    void shouldKeepPositionOfEmptyPart() {
        assertThat(compositeValue("US", null)).isEqualTo("US" + DELIMITER);
        assertThat(compositeValue(null, "1")).isEqualTo(DELIMITER + "1");
        assertThat(compositeValue("US", null, "1")).isEqualTo("US" + DELIMITER + DELIMITER + "1");
    }

    @Test
    void shouldDistinguishEmptyPartFromLiteralNullValue() {
        assertThat(compositeValue("US", null)).isNotEqualTo(compositeValue("US", "null"));
    }

    @Test
    void shouldDistinguishEmptyPartsByPosition() {
        assertThat(compositeValue("1", null, "2")).isNotEqualTo(compositeValue("1", "2", null));
    }

    @Test
    void shouldBeEmptyWhenAllPartsAreEmpty() {
        var builder = new IdValueBuilder(true);

        builder.part(null, idEntry("country", null));
        builder.part(null, idEntry("localId", null));

        assertThat(builder.isEmpty()).isTrue();
        assertThat(builder.value()).isNull();
    }

    @Test
    void shouldStoreNamedPartsAsProperties() {
        var builder = new IdValueBuilder(true);

        builder.part("US", idEntry("country", null));
        builder.part("1", idEntry(null, null));

        assertThat(builder.idPropertyValues()).containsExactly(new IdValueBuilder.Part("country", "US"));
    }

    @Test
    void shouldNotStoreEmptyPartAsProperty() {
        var builder = new IdValueBuilder(true);

        builder.part("US", idEntry("country", null));
        builder.part(null, idEntry("localId", null));

        assertThat(builder.idPropertyValues()).containsExactly(new IdValueBuilder.Part("country", "US"));
    }

    @Test
    void shouldRememberGroupOfParts() {
        var builder = new IdValueBuilder(true);
        var entry = idEntry("country", "MyGroup");

        builder.part("US", entry);

        assertThat(builder.group()).isEqualTo(entry.group());
    }

    @Test
    void shouldRememberGroupOfEmptyPart() {
        var builder = new IdValueBuilder(true);
        var entry = idEntry("country", "MyGroup");

        builder.part(null, entry);

        assertThat(builder.group()).isEqualTo(entry.group());
    }

    @Test
    void shouldFailOnPartsFromDifferentGroups() {
        var builder = new IdValueBuilder(true);
        builder.part("US", idEntry("country", "MyGroup"));

        assertThatThrownBy(() -> builder.part("1", idEntry("localId", "MyOtherGroup")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Multiple ID columns for different groups");
    }

    @Test
    void shouldResetOnClear() {
        var builder = new IdValueBuilder(true);
        builder.part("US", idEntry("country", "MyGroup"));
        builder.part("1", idEntry("localId", "MyGroup"));

        builder.clear();

        assertThat(builder.isEmpty()).isTrue();
        assertThat(builder.value()).isNull();
        assertThat(builder.group()).isNull();
        assertThat(builder.idPropertyValues()).isEmpty();

        builder.part("SE", idEntry("country", "MyOtherGroup"));
        builder.part(null, idEntry("localId", "MyOtherGroup"));

        assertThat(builder.value()).isEqualTo("SE" + DELIMITER);
        assertThat(builder.idPropertyValues()).containsExactly(new IdValueBuilder.Part("country", "SE"));
    }

    private String compositeValue(Object... partValues) {
        var builder = new IdValueBuilder(true);
        for (int i = 0; i < partValues.length; i++) {
            builder.part(partValues[i], idEntry("part" + i, null));
        }
        return (String) builder.value();
    }

    private Header.Entry idEntry(String name, String groupName) {
        return new Header.Entry(name, Type.ID, groups.getOrCreate(groupName), extractors.string());
    }
}
