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
package org.neo4j.server.modules;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.server.web.Injectable.injectable;

import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.CommunityNeoWebServer;
import org.neo4j.server.config.AuthConfigProvider;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.queryapi.versioning.QueryVersionService;
import org.neo4j.server.rest.discovery.DiscoverableURIs;
import org.neo4j.server.rest.discovery.DiscoveryService;
import org.neo4j.server.rest.repr.CommunityAuthConfigProvider;
import org.neo4j.server.web.Injectable;
import org.neo4j.server.web.WebServer;

public class DBMSModuleTest {
    @Test
    public void shouldRegisterAtRootByDefault() throws Exception {
        WebServer webServer = mock(WebServer.class);
        Config config = mock(Config.class);

        CommunityNeoWebServer neoServer = mock(CommunityNeoWebServer.class);
        when(neoServer.getBaseUri()).thenReturn(new URI("http://localhost:7575"));
        when(neoServer.getWebServer()).thenReturn(webServer);
        when(config.get(GraphDatabaseSettings.auth_enabled)).thenReturn(true);
        when(config.get(ServerSettings.http_paths_blacklist)).thenReturn(emptyList());

        var module = new DBMSModule(
                webServer,
                config,
                () -> new DiscoverableURIs.Builder(null).build(),
                NullLogProvider.getInstance(),
                new CommunityAuthConfigProvider(),
                new QueryVersionService.Builder().build());

        module.start();

        verify(webServer).addJAXRSClasses(anyList(), anyString(), isNull());
    }

    @Test
    public void shouldRegisterDiscoveryService() throws Exception {
        WebServer webServer = mock(WebServer.class);
        Config config = mock(Config.class);

        CommunityNeoWebServer neoServer = mock(CommunityNeoWebServer.class);
        when(neoServer.getBaseUri()).thenReturn(new URI("http://localhost:7575"));
        when(neoServer.getWebServer()).thenReturn(webServer);
        when(config.get(GraphDatabaseSettings.auth_enabled)).thenReturn(true);
        when(config.get(ServerSettings.http_paths_blacklist)).thenReturn(emptyList());

        var discoverableURIs = new DiscoverableURIs.Builder(null).build();
        var queryVersionService = new QueryVersionService.Builder().build();
        var authProvider = new CommunityAuthConfigProvider();
        var injectablesCapture = ArgumentCaptor.forClass(List.class);

        var module = new DBMSModule(
                webServer,
                config,
                () -> discoverableURIs,
                NullLogProvider.getInstance(),
                authProvider,
                queryVersionService);

        module.start();

        verify(webServer)
                .addJAXRSClasses(eq(singletonList(DiscoveryService.class)), eq("/"), injectablesCapture.capture());

        var injectedClasses = (List<Class<?>>) injectablesCapture.getValue().stream()
                .map(i -> {
                    if (i instanceof Injectable<?> injectable) {
                        return injectable.getType();
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        assertThat(injectedClasses)
                .containsExactlyInAnyOrder(DiscoverableURIs.class, QueryVersionService.class, AuthConfigProvider.class);

        for (var i : injectablesCapture.getValue()) {
            if (i instanceof Injectable<?> injectable) {
                if (injectable.getType().equals(DiscoverableURIs.class)) {
                    assertThat(injectable.getValue()).isEqualTo(discoverableURIs);
                } else if (injectable.getType().equals(QueryVersionService.class)) {
                    assertThat(injectable.getValue()).isEqualTo(queryVersionService);
                } else if (injectable.getType().equals(AuthConfigProvider.class)) {
                    assertThat(injectable.getValue()).isEqualTo(authProvider);
                }
            }
        }
    }
}
