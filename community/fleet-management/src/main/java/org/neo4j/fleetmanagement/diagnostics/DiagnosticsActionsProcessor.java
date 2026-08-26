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

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseContextProvider;
import org.neo4j.fleetmanagement.actions.AbstractFleetActionsProcessor;
import org.neo4j.fleetmanagement.actions.FleetAction;
import org.neo4j.fleetmanagement.communication.DiagnosticsService;
import org.neo4j.fleetmanagement.utils.Logger;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.diagnostics.EmbeddedDiagnosticsLiveConnection;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.internal.LogService;

/**
 * Fleet management service that produces a diagnostics report about the local instance. Authenticated classifiers
 * (indexes, graph counts, databases, servers) are collected through an {@link EmbeddedDiagnosticsLiveConnection}, which
 * runs the queries directly against the embedded databases - so, unlike the command-line {@code report} tool, no Bolt
 * connection or credentials are needed. Uploads the generated report to a shareable Cloud bucket for further processing.
 */
public class DiagnosticsActionsProcessor extends AbstractFleetActionsProcessor implements Lifecycle {
    private static final long AWAIT_TERMINATION_SECONDS = 10;
    private static final AtomicInteger THREAD_COUNTER = new AtomicInteger(0);

    private final DiagnosticsService diagnosticsService;
    private final GenerateAndUploadDiagnosticsReport diagnosticsReport;

    private ExecutorService diagnosticsExecutorService;

    public DiagnosticsActionsProcessor(
            LogService logService,
            Config config,
            FileSystemAbstraction fs,
            DatabaseManagementService databaseManagementService,
            DatabaseContextProvider<DatabaseContext> databaseContextProvider,
            org.neo4j.fleetmanagement.communication.DiagnosticsService diagnosticsService) {
        this.diagnosticsService = diagnosticsService;
        this.diagnosticsReport = new GenerateAndUploadDiagnosticsReport(
                logService, config, fs, databaseManagementService, databaseContextProvider);
    }

    @Override
    public void init() {}

    @Override
    public void start() {
        this.diagnosticsExecutorService = Executors.newCachedThreadPool(diagnosticsThreadFactory());
    }

    @Override
    public void stop() {
        shutdownExecutor();
    }

    @Override
    public void shutdown() {
        shutdownExecutor();
    }

    private void shutdownExecutor() {
        if (this.diagnosticsExecutorService == null) {
            return;
        }
        this.diagnosticsExecutorService.shutdownNow();
        try {
            this.diagnosticsExecutorService.awaitTermination(AWAIT_TERMINATION_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        this.diagnosticsExecutorService = null;
    }

    private static ThreadFactory diagnosticsThreadFactory() {
        return runnable -> {
            var thread = Executors.defaultThreadFactory().newThread(runnable);
            thread.setName("neo4j.FleetManagement.diagnostics-" + THREAD_COUNTER.incrementAndGet());
            thread.setDaemon(true);
            thread.setUncaughtExceptionHandler((t, e) -> Logger.getNeo4jLogger()
                    .warn(
                            "Uncaught exception in FleetManagement diagnostics thread: %s",
                            ExceptionUtils.getStackTrace(e)));
            return thread;
        };
    }

    @Override
    public void process(List<FleetAction> actions) {
        if (this.diagnosticsExecutorService == null || this.diagnosticsExecutorService.isShutdown()) {
            throw new RuntimeException("Diagnostics processor unable to process actions because it is not running");
        }
        for (FleetAction action : actions) {
            if (action.type == FleetAction.Type.GENERATE_AND_UPLOAD_DIAGNOSTICS_REPORT) {
                this.diagnosticsExecutorService.submit(
                        () -> diagnosticsService.addResult(diagnosticsReport.handle(action)));
            }
        }
    }
}
