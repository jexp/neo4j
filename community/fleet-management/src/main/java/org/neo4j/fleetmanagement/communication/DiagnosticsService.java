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
package org.neo4j.fleetmanagement.communication;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.neo4j.dbms.identity.ServerIdentity;
import org.neo4j.fleetmanagement.actions.FleetActionResult;
import org.neo4j.fleetmanagement.bootstrap.FleetManagerTask;
import org.neo4j.fleetmanagement.communication.model.UpdateDiagnosticReportStatusMessage;
import org.neo4j.fleetmanagement.communication.upstream.Upstream;
import org.neo4j.fleetmanagement.configuration.ClusterSync;
import org.neo4j.fleetmanagement.configuration.Configuration;
import org.neo4j.fleetmanagement.configuration.State;
import org.neo4j.fleetmanagement.transactions.ITransactor;

public class DiagnosticsService extends AbstractReportingService {
    private static final int FAILED_REPORT_RESULTS_LIMIT = 20;
    private static final int FAILED_REPORT_RETRIES = 3;

    private final ServerIdentity serverIdentity;

    private final ConcurrentLinkedQueue<FleetActionResult> resultQueue;
    private final ConcurrentLinkedQueue<FleetActionResult> failedToReportQueue;

    private final AtomicInteger failedToReportCounter = new AtomicInteger(0);

    public DiagnosticsService(
            ITransactor transactor,
            Upstream upstream,
            State state,
            Configuration configuration,
            ServerIdentity serverIdentity) {
        super(transactor, upstream, state, configuration);
        this.serverIdentity = serverIdentity;
        this.resultQueue = new ConcurrentLinkedQueue<>();
        this.failedToReportQueue = new ConcurrentLinkedQueue<>();
    }

    @Override
    public void report() {
        if (this.resultQueue.isEmpty()) {
            if (!failedToReportQueue.isEmpty()) {
                handleFailedToReport();
            }
            return;
        }

        // it works because queue is polled only here
        var size = this.resultQueue.size();
        List<FleetActionResult> results = new ArrayList<>();
        while (results.size() < size && !resultQueue.isEmpty()) {
            results.add(this.resultQueue.poll());
        }

        try {
            reportResults(results);
        } catch (Exception e) {
            addFailedToReport(results);
            throw e;
        }
    }

    private void handleFailedToReport() {
        if (failedToReportCounter.get() <= FAILED_REPORT_RETRIES) {
            try {
                reportResults(failedToReportQueue.stream().toList());
                failedToReportCounter.set(0);
                failedToReportQueue.clear();
            } catch (Exception e) {
                failedToReportCounter.incrementAndGet();
            }
        } else {
            failedToReportQueue.clear();
            failedToReportCounter.set(0);
        }
    }

    private void addFailedToReport(List<FleetActionResult> results) {
        failedToReportCounter.incrementAndGet();

        if (results.size() > FAILED_REPORT_RESULTS_LIMIT
                || failedToReportQueue.size() + results.size() > FAILED_REPORT_RESULTS_LIMIT) {
            failedToReportQueue.clear();
            // reverse to only include latest failed
            var iter = results.reversed().iterator();
            while (failedToReportQueue.size() < FAILED_REPORT_RESULTS_LIMIT) {
                failedToReportQueue.add(iter.next());
            }
            return;
        }

        for (var r : results) {
            if (!failedToReportQueue.contains(r)) {
                failedToReportQueue.add(r);
            }
        }
    }

    public void reportResults(List<FleetActionResult> results) {
        var updateDiagnosticReportStatusMsg = new UpdateDiagnosticReportStatusMessage();
        updateDiagnosticReportStatusMsg.serverId =
                serverIdentity.serverId().uuid().toString();
        updateDiagnosticReportStatusMsg.actionResults = results;
        callApi(updateDiagnosticReportStatusMsg, Upstream.Endpoint.DIAGNOSTICS_REPORT);
    }

    public void addResult(FleetActionResult result) {
        resultQueue.add(result);
    }

    public static class DiagnosticsServiceTask extends FleetManagerTask {
        private final DiagnosticsService diagnosticsService;

        public DiagnosticsServiceTask(State state, ClusterSync clusterSync, DiagnosticsService diagnosticsService) {
            super(state, clusterSync);
            this.diagnosticsService = diagnosticsService;
        }

        @Override
        protected void execute() {
            if (state.isActive()) {
                this.diagnosticsService.report();
            }
        }
    }
}
