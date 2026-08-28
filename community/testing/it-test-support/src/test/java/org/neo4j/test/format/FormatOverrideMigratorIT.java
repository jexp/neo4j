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
package org.neo4j.test.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.system_database_format;
import static org.neo4j.configuration.GraphDatabaseSettings.db_format;
import static org.neo4j.test.format.FormatOverrideMigrator.OVERRIDE_STORE_FORMAT_KEY;
import static org.neo4j.test.format.FormatOverrideMigrator.OVERRIDE_SYSTEM_STORE_FORMAT_KEY;

import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.neo4j.configuration.Config;
import org.neo4j.test.extension.RequireAlignedFormat;

@Isolated
class FormatOverrideMigratorIT {
    private String originalFormatOverride;
    private String originalSystemFormatOverride;

    @BeforeEach
    void clearOverrides() {
        originalFormatOverride = System.clearProperty(OVERRIDE_STORE_FORMAT_KEY);
        originalSystemFormatOverride = System.clearProperty(OVERRIDE_SYSTEM_STORE_FORMAT_KEY);
    }

    @AfterEach
    void restoreOverrides() {
        restore(OVERRIDE_STORE_FORMAT_KEY, originalFormatOverride);
        restore(OVERRIDE_SYSTEM_STORE_FORMAT_KEY, originalSystemFormatOverride);
    }

    @Test
    @RequireAlignedFormat
    void noOverridesKeepDeclaredDefaults() {
        Config config = Config.defaults();

        assertThat(config.get(db_format)).isEqualTo(db_format.defaultValue());
        assertThat(config.get(system_database_format)).isEqualTo(system_database_format.defaultValue());
    }

    @Test
    void overridesReplaceDefaultsOfMatchingSettings() {
        System.setProperty(OVERRIDE_STORE_FORMAT_KEY, "standard");
        System.setProperty(OVERRIDE_SYSTEM_STORE_FORMAT_KEY, "multiversion_block");

        Config config = Config.defaults();

        assertThat(config.get(db_format)).isEqualTo("standard");
        assertThat(config.get(system_database_format)).isEqualTo("multiversion_block");
        assertThat(config.isExplicitlySet(db_format)).isFalse();
        assertThat(config.isExplicitlySet(system_database_format)).isFalse();
    }

    @Test
    void explicitlySetValuesWinOverOverrides() {
        System.setProperty(OVERRIDE_STORE_FORMAT_KEY, "standard");
        System.setProperty(OVERRIDE_SYSTEM_STORE_FORMAT_KEY, "standard");

        Config config = Config.newBuilder()
                .set(db_format, "block")
                .set(system_database_format, "block")
                .build();

        assertThat(config.get(db_format)).isEqualTo("block");
        assertThat(config.get(system_database_format)).isEqualTo("block");
    }

    @Test
    void explicitlySetRawValuesWinOverOverrides() {
        System.setProperty(OVERRIDE_STORE_FORMAT_KEY, "standard");
        System.setProperty(OVERRIDE_SYSTEM_STORE_FORMAT_KEY, "standard");

        Config config = Config.newBuilder()
                .setRaw(Map.of(db_format.name(), "block", system_database_format.name(), "block"))
                .build();

        assertThat(config.get(db_format)).isEqualTo("block");
        assertThat(config.get(system_database_format)).isEqualTo("block");
    }

    private static void restore(String key, String originalValue) {
        if (originalValue != null) {
            System.setProperty(key, originalValue);
        } else {
            System.clearProperty(key);
        }
    }
}
