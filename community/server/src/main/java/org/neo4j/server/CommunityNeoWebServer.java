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
package org.neo4j.server;

import java.util.ArrayList;
import java.util.function.Supplier;
import org.neo4j.bolt.tx.TransactionManager;
import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.routing.ClientRoutingDomainChecker;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.MemoryPools;
import org.neo4j.monitoring.Monitors;
import org.neo4j.server.config.AuthConfigProvider;
import org.neo4j.server.configuration.ConfigurableServerModules;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.http.cypher.CypherResource;
import org.neo4j.server.modules.AuthorizationModule;
import org.neo4j.server.modules.ClacksModule;
import org.neo4j.server.modules.DBMSModule;
import org.neo4j.server.modules.Neo4jBrowserModule;
import org.neo4j.server.modules.QueryModule;
import org.neo4j.server.modules.ServerModule;
import org.neo4j.server.modules.ThirdPartyJAXRSModule;
import org.neo4j.server.modules.TransactionModule;
import org.neo4j.server.queryapi.QueryResource;
import org.neo4j.server.queryapi.versioning.QueryVersionService;
import org.neo4j.server.rest.discovery.DiscoverableURIs;
import org.neo4j.server.web.JettyWebServer;
import org.neo4j.server.web.WebServer;
import org.neo4j.time.SystemNanoClock;

public class CommunityNeoWebServer extends AbstractNeoWebServer {
    public CommunityNeoWebServer(
            DatabaseManagementService managementService,
            TransactionManager transactionManager,
            Dependencies globalDependencies,
            Config config,
            InternalLogProvider userLogProvider,
            DbmsInfo dbmsInfo,
            MemoryPools memoryPools,
            Monitors monitors,
            SystemNanoClock clock) {
        super(
                managementService,
                globalDependencies,
                config,
                userLogProvider,
                dbmsInfo,
                memoryPools,
                transactionManager,
                monitors,
                clock);
    }

    @Override
    protected final Iterable<ServerModule> createServerModules() {
        var config = getConfig();
        var enabledModules = config.get(ServerSettings.http_enabled_modules);
        var serverModules = new ArrayList<ServerModule>();
        if (!enabledModules.isEmpty()) {
            var clientRoutingDomainChecker =
                    getGlobalDependencies().resolveDependency(ClientRoutingDomainChecker.class);
            var discoverableURIsBuilder = new DiscoverableURIs.Builder(clientRoutingDomainChecker);
            var queryVersionServiceBuilder = new QueryVersionService.Builder();

            if (enabledModules.contains(ConfigurableServerModules.TRANSACTIONAL_ENDPOINTS)) {
                discoverableURIsBuilder.addEndpoint(
                        CypherResource.NAME, CypherResource.absoluteDatabaseTransactionPath(config));
                serverModules.add(new TransactionModule(webServer, config, clock));
            }

            if (enabledModules.contains(ConfigurableServerModules.UNMANAGED_EXTENSIONS)) {
                serverModules.add(new ThirdPartyJAXRSModule(webServer, config, userLogProvider));
            }
            if (enabledModules.contains(ConfigurableServerModules.BROWSER)) {
                var webDir = config.get(ServerSettings.web_dir_path);
                var browserGlob =
                        config.get(ServerSettings.browser_matching_pattern).getFirst();
                serverModules.add(new Neo4jBrowserModule(webServer, webDir, browserGlob));
            }

            if (enabledModules.contains(ConfigurableServerModules.QUERY_API_ENDPOINTS)) {
                discoverableURIsBuilder.addEndpoint(
                        QueryResource.NAME_V2, QueryResource.absoluteDatabaseTransactionPathV2(config));
                discoverableURIsBuilder.addEndpoint(
                        QueryResource.NAME, QueryResource.absoluteDatabaseTransactionPath(config));
                queryVersionServiceBuilder.withVersion(QueryResource.VERSION);

                serverModules.add(new QueryModule(webServer, config, metricsMonitor));
            }

            for (var extended : extendModules(discoverableURIsBuilder)) {
                serverModules.add(extended);
            }

            serverModules.add(createAuthorizationModule());

            serverModules.addFirst(createDBMSModule(
                    // defer the creation of bolt endpoint since the bolt ports are only
                    // available after the service starts
                    () -> discoverableURIsBuilder
                            .addBoltEndpoint(config, connectorPortRegister)
                            .build(),
                    queryVersionServiceBuilder.build()));
        }

        if (config.get(ServerSettings.clacks_enabled)) {
            serverModules.add(new ClacksModule(webServer, config.get(ServerSettings.clacks_names)));
        }

        return serverModules;
    }

    protected Iterable<ServerModule> extendModules(DiscoverableURIs.Builder discoverableURIs) {
        return new ArrayList<>();
    }

    @Override
    protected WebServer createWebServer() {
        var globalDependencies = getGlobalDependencies();
        var connectionTracker = globalDependencies.resolveDependency(NetworkConnectionTracker.class);
        var webServer = new JettyWebServer(userLogProvider, getConfig(), connectionTracker, byteBufferPool);
        globalDependencies.satisfyDependency(webServer);
        return webServer;
    }

    protected final DBMSModule createDBMSModule(
            Supplier<DiscoverableURIs> discoverableURIsSupplier, QueryVersionService queryVersionService) {
        // Bolt port isn't available until runtime, so defer loading until then
        var authConfigProvider = getGlobalDependencies().resolveDependency(AuthConfigProvider.class);
        return new DBMSModule(
                webServer,
                getConfig(),
                discoverableURIsSupplier,
                userLogProvider,
                authConfigProvider,
                queryVersionService);
    }

    protected AuthorizationModule createAuthorizationModule() {
        return new AuthorizationModule(webServer, authManagerSupplier, userLogProvider, getConfig(), getUriWhitelist());
    }
}
