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
package org.neo4j.test;

import java.util.Map;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.SettingMigrator;
import org.neo4j.logging.InternalLog;

@ServiceProvider
public class LogFormatOverrideMigrator implements SettingMigrator {
    public static final String OVERRIDE_LOG_FORMAT_KEY = "NEO4J_OVERRIDE_LOG_FORMAT";
    public static final String ENVELOPES = "envelopes";
    public static final String OVERRIDE_RAFT_LOG_IMPLEMENTATION_KEY = "NEO4J_OVERRIDE_RAFT_LOG_IMPL";
    public static final String ENVELOPED = "enveloped";
    public static final String MERGED = "merged";
    public static final String MERGED_ON_LATEST = "merged_on_latest";

    @Override
    public void migrate(Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
        String overrideLogFormat = System.getProperty(OVERRIDE_LOG_FORMAT_KEY);
        if (ENVELOPES.equals(overrideLogFormat)) {
            try {
                defaultValues.put(
                        GraphDatabaseInternalSettings.allow_new_log_format_on_upgrade_or_create.name(), "true");
            } catch (RuntimeException ex) {
                log.warn("Unable to override the log format to " + overrideLogFormat, ex);
            }
        }
        // For public kernel classes enable sufficient flags to move to V11 LogFormat
        String logImplementation = System.getProperty(OVERRIDE_RAFT_LOG_IMPLEMENTATION_KEY);
        if (MERGED.equals(logImplementation) || MERGED_ON_LATEST.equals(logImplementation)) {
            defaultValues.put(GraphDatabaseInternalSettings.merged_log.name(), "true");
        }
    }
}
