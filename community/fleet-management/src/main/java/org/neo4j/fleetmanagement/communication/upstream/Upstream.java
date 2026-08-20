package org.neo4j.fleetmanagement.communication.upstream;

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

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.Socket;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Date;
import javax.net.ssl.SSLSocketFactory;
import org.neo4j.fleetmanagement.communication.Helpers;
import org.neo4j.fleetmanagement.configuration.State;
import org.neo4j.fleetmanagement.topology.TopologyMapper;
import org.neo4j.fleetmanagement.transactions.ITransactor;
import org.neo4j.fleetmanagement.utils.TokenUtils;
import org.neo4j.logging.Log;

public class Upstream {
    public enum Endpoint {
        REPORTING,
        QUERIES,
        METRICS,
        CONNECT,
        CONFIG,
        PING,
        SECURITY_LOGS,
        MIGRATION_IMPORT_TOKEN,
        MIGRATION_UPDATE_STATUS,
    }

    private static final String DEFAULT_BASE_URL = "https://fleet-management-api.neo4j.io/api/v1";

    private static long maxTokenAge = 59 * 60 * 1000L; // 59 minutes

    private volatile String baseUrl = DEFAULT_BASE_URL;

    private final ITransactor transactor;
    private ApiKeyProvider.ApiKey apiKey;
    private ApiTokenGenerator apiTokenGenerator;
    private long generatedAt;

    private final Log userLog;
    private final State state;

    private volatile String apiToken;

    public Upstream(ITransactor transactor, Log userLog, State state) {
        this.transactor = transactor;
        this.userLog = userLog;
        this.state = state;
    }

    public void setConnectionUrl(String connectionUrl) {
        this.baseUrl = (connectionUrl == null || connectionUrl.isEmpty()) ? DEFAULT_BASE_URL : connectionUrl;
    }

    public String getConnectionUrl() {
        return this.baseUrl;
    }

    public boolean isConnectionUrlDefault() {
        return this.baseUrl.equals(DEFAULT_BASE_URL);
    }

    public void setToken(String token) throws IOException {
        if (token == null || token.isEmpty()) {
            apiToken = null;
        } else {
            var apiKey = TokenUtils.parseToken(token);
            ApiKeyProvider apiKeyProvider = new ApiKeyProvider(apiKey);
            this.apiTokenGenerator = new ApiTokenGenerator(apiKeyProvider);
            this.apiKey = apiKey;
            generateToken();
        }
    }

    private String getToken() {
        if (this.state.isActive()
                && (this.apiToken == null || this.generatedAt + maxTokenAge < System.currentTimeMillis())) {
            generateToken();
        }

        return this.apiToken;
    }

    public ApiKeyProvider.ApiKey getApiKey() {
        return apiKey;
    }

    public void generateToken() {
        var tokenExpiry = new Date(System.currentTimeMillis() + 3600_000L);
        this.apiToken = apiTokenGenerator.generate(new ApiTokenGenerator.ApiTokenClaims(
                "plugin", "plugin", "fleet-management-api", tokenExpiry, apiKey.projectId, getDbmsId()));
        this.generatedAt = System.currentTimeMillis();
    }

    public String getDbmsId() {
        return TopologyMapper.getDbmsId(transactor::getDatabases);
    }

    public UpstreamPostRequest postTo(Endpoint endpoint) throws IOException {
        return switch (endpoint) {
            case CONNECT -> postTo("plugin/connect");
            case REPORTING -> postTo("plugin/report");
            case QUERIES -> postTo("plugin/queries");
            case METRICS -> postTo("plugin/metrics");
            case CONFIG -> postTo("plugin/config");
            case PING -> postTo("plugin/ping");
            case SECURITY_LOGS -> postTo("plugin/security-logs");
            case MIGRATION_IMPORT_TOKEN -> postTo("plugin/migration-to-aura/import-token");
            case MIGRATION_UPDATE_STATUS -> postTo("plugin/migration-to-aura/status");
        };
    }

    public boolean isReachable() {
        URL url;
        try {
            url = new URL(this.baseUrl);
        } catch (MalformedURLException e) {
            return false;
        }

        try (Socket soc = SSLSocketFactory.getDefault().createSocket()) {
            soc.connect(new InetSocketAddress(url.getHost(), url.getDefaultPort()), 5000);
        } catch (UnknownHostException ex) {
            userLog.error(
                    "Fleet manager failed to reach the API - UnknownHostException: Please verify that the server can connect to a DNS server",
                    ex.getMessage());
            return false;
        } catch (IOException ex) {
            userLog.error(
                    "Fleet manager failed to reach the API - IOException: Consider checking the connection and if failures continue, try again later.",
                    ex.getMessage());
            return false;
        } catch (SecurityException ex) {
            userLog.error(
                    "Fleet manager failed to reach the API - SecurityException: A security manager exists and its checkConnect method doesn't allow connecting to the API.",
                    ex.getMessage());
            return false;
        } catch (IllegalArgumentException ex) {
            userLog.error(
                    "Fleet manager failed to reach the API - IllegalArgumentException: Please verify access to the public internet e.g., check your proxy settings and firewall rules.",
                    ex.getMessage());
            return false;
        }
        return true;
    }

    private UpstreamPostRequest postTo(String path) throws IOException {
        var url = new URL(String.format("%s/%s", this.baseUrl, path));
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();

        conn.setRequestProperty("Authorization", String.format("Bearer %s", getToken()));

        return new UpstreamPostRequest(conn);
    }

    public static class UpstreamPostRequest {
        private final HttpURLConnection conn;

        private UpstreamPostRequest(HttpURLConnection conn) throws ProtocolException {
            conn.setDoOutput(true);
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            this.conn = conn;
        }

        public int transmit(byte[] data) throws IOException {
            try (OutputStream os = conn.getOutputStream()) {
                os.write(data);
                os.flush();
            } catch (IOException e) {
                throw new IOException("Exception while writing request data: ", e);
            }

            try {
                return conn.getResponseCode();
            } catch (IOException e) {
                throw new IOException("Exception while getting error code: ", e);
            }
        }

        public byte[] getResponseBody() throws IOException {
            int responseCode;
            try {
                responseCode = conn.getResponseCode();
            } catch (IOException e) {
                throw new IOException("Exception while getting the response code: ", e);
            }

            if (Helpers.responseOk(responseCode)) {
                try {
                    return conn.getInputStream().readAllBytes();
                } catch (IOException e) {
                    throw new IOException("Exception while reading response data: ", e);
                }
            }

            try {
                return conn.getErrorStream().readAllBytes();
            } catch (IOException e) {
                throw new IOException("Exception while reading error response data: ", e);
            }
        }
    }
}
