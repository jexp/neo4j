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
package org.neo4j.fleetmanagement.diagnostics;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HexFormat;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.neo4j.commandline.dbms.DiagnosticsReportGenerator;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseContextProvider;
import org.neo4j.fleetmanagement.actions.FleetAction;
import org.neo4j.fleetmanagement.actions.FleetActionHandler;
import org.neo4j.fleetmanagement.actions.FleetActionResult;
import org.neo4j.fleetmanagement.communication.Helpers;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.diagnostics.DiagnosticsLiveConnection;
import org.neo4j.kernel.diagnostics.DiagnosticsReporterProgress;
import org.neo4j.kernel.diagnostics.EmbeddedDiagnosticsLiveConnection;
import org.neo4j.logging.internal.LogService;

public class GenerateAndUploadDiagnosticsReport extends FleetActionHandler {
    private static final int UPLOAD_CONNECTION_TIMEOUT =
            (int) Duration.of(10, ChronoUnit.SECONDS).toMillis();
    private static final int UPLOAD_READ_TIMEOUT =
            (int) Duration.of(120, ChronoUnit.SECONDS).toMillis();
    private static final Pattern UPLOAD_DESTINATION_PATTERN = Pattern.compile(
            "^(?<url>https://storage\\.googleapis\\.com/[a-z0-9]+-fleet-management-api-diagnostic-reports/.*)\\?(?<query>[\\S\\H]+)$");
    private static final Set<String> REPORT_CLASSIFIERS = Set.of(
            "logs",
            "config",
            "plugins",
            "tree",
            "metrics",
            "ps",
            "version",
            "databases",
            "servers",
            "indexes",
            "graphcounts");

    private final LogService logService;
    private final Config config;
    private final FileSystemAbstraction fs;
    private final DatabaseManagementService databaseManagementService;
    private final DatabaseContextProvider<DatabaseContext> databaseContextProvider;

    public GenerateAndUploadDiagnosticsReport(
            LogService logService,
            Config config,
            FileSystemAbstraction fs,
            DatabaseManagementService databaseManagementService,
            DatabaseContextProvider<DatabaseContext> databaseContextProvider) {
        this.logService = logService;
        this.config = config;
        this.fs = fs;
        this.databaseManagementService = databaseManagementService;
        this.databaseContextProvider = databaseContextProvider;
    }

    public FleetActionResult execute(FleetAction action) {
        var log = logService.getUserLog(DiagnosticsReportGenerator.class);
        var result = new FleetActionResult(action.id, FleetActionResult.Status.running, null, null);
        GenerateAndUploadReportActionPayload payload;
        try {
            payload = action.deserializePayload(GenerateAndUploadReportActionPayload.class);
        } catch (JsonProcessingException e) {
            log.error("Unable to parse payload", e);
            return new FleetActionResult(action.id, FleetActionResult.Status.failed, e.getMessage(), null);
        }

        log.info("Generating diagnostics report...");
        File reportFile = null;
        Path reportDir = null;
        try {
            Path homeDir = config.get(GraphDatabaseSettings.neo4j_home);
            Path confDir = config.get(GraphDatabaseSettings.configuration_directory);
            var ctx = FleetDiagnosticsExecutionContext.create(homeDir, confDir, this.fs);
            reportDir = homeDir.resolve("diagnostics");
            if (!fs.isDirectory(reportDir)) {
                fs.mkdirs(reportDir);
            }

            Set<String> dbNames = databaseContextProvider.registeredDatabases().keySet().stream()
                    .map(NamedDatabaseId::name)
                    .collect(Collectors.toCollection(TreeSet::new));
            String defaultDatabase = config.get(GraphDatabaseSettings.initial_default_database);

            String reportFilePath;
            try (DiagnosticsLiveConnection connection =
                    new EmbeddedDiagnosticsLiveConnection(databaseManagementService, defaultDatabase)) {
                DiagnosticsReportGenerator generator = new DiagnosticsReportGenerator(ctx, config, false);
                reportFilePath = generator.generate(
                        REPORT_CLASSIFIERS, reportDir, DiagnosticsReporterProgress.EMPTY, true, dbNames, connection);
            }

            if (reportFilePath == null) {
                return new FleetActionResult(
                        action.id, FleetActionResult.Status.failed, ctx.errAsString(), ctx.outAsString());
            }
            reportFile = Path.of(reportFilePath).toFile();

            log.info("Diagnostics report generation completed.");

            upload(ctx, payload.uploadUrl(), reportFile, result);

            log.info("Diagnostics report upload completed: %s", ctx.getExecutionOutput());
        } catch (Exception e) {
            var redacted = redactUploadUrl(e);
            log.error("Failed to generate diagnostics report", redacted);
            return new FleetActionResult(action.id, FleetActionResult.Status.failed, redacted.getMessage(), null);
        } finally {
            cleanupReport(reportFile, reportDir);
        }

        return result;
    }

    public void upload(
            FleetDiagnosticsExecutionContext ctx, String signedUrl, File fileToUpload, FleetActionResult result)
            throws IOException, URISyntaxException, NoSuchAlgorithmException {
        var uploadUrlMatcher = UPLOAD_DESTINATION_PATTERN.matcher(signedUrl);
        if (!uploadUrlMatcher.matches()) {
            throw new IllegalArgumentException("Invalid upload url");
        }

        var connection = getConnection(signedUrl, fileToUpload);
        var digester = MessageDigest.getInstance("SHA-256");
        try {
            var outputStream = connection.getOutputStream();
            try (var inputStream =
                    new DigestInputStream(new BufferedInputStream(new FileInputStream(fileToUpload)), digester)) {
                inputStream.transferTo(outputStream);
            }

            var responseCode = connection.getResponseCode();

            if (!Helpers.responseOk(responseCode)) {
                var errorStream = connection.getErrorStream();
                if (errorStream != null) {
                    errorStream.transferTo(ctx.getErr());
                }

                userLog.error("Error uploading report: " + ctx.errAsString());
                result.serializeError(Map.of("responseCode", responseCode, "logs", ctx.errAsString()));
                result.status = FleetActionResult.Status.failed;
            } else {
                var output = connection.getResponseMessage();
                result.serializeOutput(Map.of("checksum", HexFormat.of().formatHex(digester.digest()), "logs", output));
                result.status = FleetActionResult.Status.success;
            }
        } finally {
            connection.disconnect();
        }
    }

    private static HttpURLConnection getConnection(String signedUrl, File fileToUpload)
            throws URISyntaxException, IOException {
        var fileLength = fileToUpload.length();
        var uploadUrl = new URI(signedUrl);
        var connection = (HttpURLConnection) uploadUrl.toURL().openConnection();
        connection.setFixedLengthStreamingMode(fileLength);
        connection.setRequestProperty("Content-Type", "application/zip");
        connection.setRequestProperty("Content-Length", String.valueOf(fileLength));
        connection.setRequestMethod("PUT");
        connection.setDoOutput(true);
        connection.setConnectTimeout(UPLOAD_CONNECTION_TIMEOUT);
        connection.setReadTimeout(UPLOAD_READ_TIMEOUT);
        connection.connect();
        return connection;
    }

    private void cleanupReport(File reportFile, Path reportDir) {
        if (reportFile != null && reportFile.exists() && !reportFile.delete()) {
            userLog.error("Could not cleanup report: " + reportFile.getAbsolutePath());
        }
        if (reportDir != null) {
            // best-effort: delete() on a directory only succeeds when it is empty
            reportDir.toFile().delete();
        }
    }

    private static Exception redactUploadUrl(Exception e) {
        if (e == null || e.getMessage() == null) {
            return e;
        }
        var matcher = UPLOAD_DESTINATION_PATTERN.matcher(e.getMessage());
        var redactedException = new Exception(matcher.replaceAll("<redacted>"));
        redactedException.setStackTrace(e.getStackTrace());
        return redactedException;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record GenerateAndUploadReportActionPayload(String uploadUrl) {}
}
