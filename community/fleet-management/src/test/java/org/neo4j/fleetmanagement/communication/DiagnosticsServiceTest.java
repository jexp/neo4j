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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatRuntimeException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.dbms.identity.ServerIdentity;
import org.neo4j.fleetmanagement.actions.FleetActionResult;
import org.neo4j.fleetmanagement.communication.model.UpdateDiagnosticReportStatusMessage;
import org.neo4j.fleetmanagement.communication.upstream.Upstream;
import org.neo4j.fleetmanagement.configuration.Configuration;
import org.neo4j.fleetmanagement.configuration.State;
import org.neo4j.fleetmanagement.transactions.ITransactor;
import org.neo4j.fleetmanagement.utils.Logger;
import org.neo4j.logging.Log;

class DiagnosticsServiceTest {
    private static final String SERVER_ID = "11111111-1111-1111-1111-111111111111";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private Upstream mockUpstream;
    private Upstream.UpstreamPostRequest mockPostRequest;
    private DiagnosticsService service;

    @BeforeAll
    static void beforeAll() {
        Logger.initLogger(mock(Log.class));
    }

    @BeforeEach
    void setUp() throws IOException {
        mockUpstream = mock(Upstream.class);
        mockPostRequest = mock(Upstream.UpstreamPostRequest.class);
        when(mockUpstream.postTo(Upstream.Endpoint.DIAGNOSTICS_REPORT)).thenReturn(mockPostRequest);

        var state = new State();
        state.setConnected();
        ServerIdentity serverIdentity = () -> ServerId.from(SERVER_ID).orElseThrow();
        service = new DiagnosticsService(
                mock(ITransactor.class), mockUpstream, state, mock(Configuration.class), serverIdentity);
    }

    private static FleetActionResult result(String id) {
        return new FleetActionResult(id, FleetActionResult.Status.success, null, null);
    }

    private List<UpdateDiagnosticReportStatusMessage> transmittedMessages() throws IOException {
        var messages = new ArrayList<UpdateDiagnosticReportStatusMessage>();
        for (var invocation : mockingDetails(mockPostRequest).getInvocations()) {
            if (invocation.getMethod().getName().equals("transmit")) {
                byte[] payload = invocation.getArgument(0);
                messages.add(MAPPER.readValue(
                        new String(payload, StandardCharsets.UTF_8), UpdateDiagnosticReportStatusMessage.class));
            }
        }
        return messages;
    }

    @Test
    void reportSendsQueuedResultsAndDrainsQueue() throws IOException {
        when(mockPostRequest.transmit(any(byte[].class))).thenReturn(200);
        service.addResult(result("a1"));
        service.addResult(result("a2"));

        service.report();

        assertThat(transmittedMessages()).singleElement().satisfies(msg -> {
            assertThat(msg.serverId).isEqualTo(SERVER_ID);
            assertThat(msg.actionResults).containsExactly(result("a1"), result("a2"));
        });

        service.report();
        assertThat(transmittedMessages()).hasSize(1);
    }

    @Test
    void reportWithEmptyQueuesDoesNothing() {
        service.report();

        verifyNoInteractions(mockUpstream);
    }

    @Test
    void failedReportIsRetriedOnNextReport() throws IOException {
        when(mockPostRequest.transmit(any(byte[].class)))
                .thenThrow(new IOException("network"))
                .thenReturn(200);
        service.addResult(result("a1"));

        assertThatRuntimeException().isThrownBy(service::report);
        service.report();

        assertThat(transmittedMessages())
                .hasSize(2)
                .allSatisfy(msg -> assertThat(msg.actionResults).containsExactly(result("a1")));
    }

    @Test
    void failedReportIsDroppedAfterRetriesAreExhausted() throws IOException {
        when(mockPostRequest.transmit(any(byte[].class))).thenThrow(new IOException("network"));
        service.addResult(result("a1"));

        assertThatRuntimeException().isThrownBy(service::report);
        for (int i = 0; i < 5; i++) {
            service.report();
        }

        // initial attempt + 3 retries, then the queued failure is dropped without another attempt
        assertThat(transmittedMessages()).hasSize(4);
    }

    @Test
    void failedReportQueueKeepsOnlyTheLatestTwentyResults() throws IOException {
        when(mockPostRequest.transmit(any(byte[].class)))
                .thenThrow(new IOException("network"))
                .thenReturn(200);
        var results = new ArrayList<FleetActionResult>();
        for (int i = 1; i <= 25; i++) {
            var r = result("a" + i);
            results.add(r);
            service.addResult(r);
        }

        assertThatRuntimeException().isThrownBy(service::report);
        service.report();

        var expected = results.subList(5, 25);
        assertThat(transmittedMessages().get(1).actionResults)
                .hasSize(20)
                .containsExactlyInAnyOrderElementsOf(expected);
    }
}
