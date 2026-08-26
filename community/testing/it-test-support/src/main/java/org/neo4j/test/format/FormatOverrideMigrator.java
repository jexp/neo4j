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

import static org.neo4j.configuration.GraphDatabaseInternalSettings.system_database_format;
import static org.neo4j.configuration.GraphDatabaseSettings.db_format;

import java.util.Map;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.SettingMigrator;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.logging.InternalLog;

@ServiceProvider
public class FormatOverrideMigrator implements SettingMigrator {
    public static final String OVERRIDE_STORE_FORMAT_KEY = "NEO4J_OVERRIDE_STORE_FORMAT";
    public static final String OVERRIDE_SYSTEM_STORE_FORMAT_KEY = "NEO4J_OVERRIDE_SYSTEM_STORE_FORMAT";

    @Override
    public void migrate(Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
        overrideDbFormat(values, defaultValues, log);
        overrideSystemDbFormat(values, defaultValues, log);
    }

    private static void overrideDbFormat(
            Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
        overrideFormatSetting(OVERRIDE_STORE_FORMAT_KEY, values, db_format, defaultValues, log);
    }

    private static void overrideSystemDbFormat(
            Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
        overrideFormatSetting(OVERRIDE_SYSTEM_STORE_FORMAT_KEY, values, system_database_format, defaultValues, log);
    }

    private static void overrideFormatSetting(
            String overrideStoreFormatKey,
            Map<String, String> values,
            Setting<String> setting,
            Map<String, String> defaultValues,
            InternalLog log) {
        String overrideValue = System.getProperty(overrideStoreFormatKey);
        if (overrideValue != null && !values.containsKey(setting.name())) {
            try {
                defaultValues.put(setting.name(), overrideValue);
            } catch (RuntimeException ex) {
                log.warn("Unable to override the setting " + setting + " to " + overrideValue, ex);
            }
        }
    }
}
