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
package org.neo4j.query.language;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.kernel.database.NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID;
import static org.neo4j.kernel.database.NamedDatabaseId.SYSTEM_DATABASE_NAME;
import static org.neo4j.test.assertion.Assert.awaitUntilAsserted;

import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.QueryLanguageConverter;
import org.neo4j.cypher.internal.CypherVersion;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.systemgraph.DefaultQueryLanguageLookup;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomSupportExtension;
import org.neo4j.test.extension.SkipOnSpd;

@RandomSupportExtension
@ImpermanentDbmsExtension(configurationCallback = "configure")
@SkipOnSpd(reason = "SET DEFAULT LANGUAGE is not allowed with a graph shard target")
public class CommunityDefaultQueryLanguageLookupIT {
    @Inject
    RandomSupport rand;

    @Inject
    GraphDatabaseAPI neo4j;

    @Inject
    DatabaseManagementService dbms;

    DefaultQueryLanguageLookup lookup;
    CypherVersion systemDefaultLanguage;

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        final var randLang = rand.among(GraphDatabaseSettings.CypherVersion.values());
        // Might need to be enabled when the next experimental version appear:
        // builder.setConfig(GraphDatabaseInternalSettings.enable_experimental_cypher_versions, true);
        builder.setConfig(GraphDatabaseSettings.default_language, randLang);
        this.systemDefaultLanguage = QueryLanguageConverter.toInternal(randLang);
    }

    @BeforeEach
    void beforeEach() {
        this.lookup = neo4j.getDependencyResolver().resolveDependency(DefaultQueryLanguageLookup.class);
    }

    @AfterEach
    void afterEach() {
        // Reset default language
        setDefaultLanguage(neo4j.databaseName(), systemDefaultLanguage);
        setDefaultLanguage(SYSTEM_DATABASE_NAME, systemDefaultLanguage);
    }

    @Test
    void lookupAfterStart() {
        assertThat(lookup.defaultLanguage(neo4j.databaseId())).contains(systemDefaultLanguage);
    }

    @ParameterizedTest
    @EnumSource(CypherVersion.class)
    void lookupAfterSetDefaultLanguage(CypherVersion language) {
        setDefaultLanguage(neo4j.databaseName(), language);
        assertDefaultLanguage(neo4j.databaseId(), language);
    }

    @ParameterizedTest
    @EnumSource(CypherVersion.class)
    void setDefaultVersionOnSystemDb(CypherVersion language) {
        setDefaultLanguage(SYSTEM_DATABASE_NAME, language);
        assertDefaultLanguage(NAMED_SYSTEM_DATABASE_ID, language);
    }

    private void assertDefaultLanguage(NamedDatabaseId id, CypherVersion expected) {
        awaitUntilAsserted(() -> assertThat(lookup.defaultLanguage(id)).contains(expected));
    }

    private void setDefaultLanguage(String dbName, CypherVersion language) {
        final var query = """
                alter database $db
                set default language cypher %s
                """.formatted(language.versionName);
        final var params = Map.<String, Object>of("db", dbName);
        dbms.database("system").executeTransactionally(query, params);
    }
}
