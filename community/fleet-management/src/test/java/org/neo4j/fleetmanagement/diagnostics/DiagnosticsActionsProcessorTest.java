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

import static org.assertj.core.api.Assertions.assertThatRuntimeException;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.fleetmanagement.actions.FleetAction;
import org.neo4j.fleetmanagement.actions.FleetActionResult;
import org.neo4j.fleetmanagement.communication.DiagnosticsService;
import org.neo4j.fleetmanagement.utils.Logger;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.NullLogService;

class DiagnosticsActionsProcessorTest {

    private DiagnosticsService diagnosticsService;
    private DiagnosticsActionsProcessor processor;

    @BeforeAll
    static void beforeAll() {
        Logger.initLogger(mock(Log.class));
    }

    @BeforeEach
    void setUp() {
        diagnosticsService = mock(DiagnosticsService.class);
        processor = new DiagnosticsActionsProcessor(
                NullLogService.getInstance(), null, null, null, null, diagnosticsService);
    }

    @AfterEach
    void tearDown() {
        processor.shutdown();
    }

    private static FleetAction diagnosticsAction(String payload) {
        return new FleetAction(
                "a1", FleetAction.Type.GENERATE_AND_UPLOAD_DIAGNOSTICS_REPORT, FleetAction.Category.SERVER, payload);
    }

    @Test
    void processThrowsWhenNotRunning() {
        assertThatRuntimeException()
                .isThrownBy(() -> processor.process(List.of(diagnosticsAction(null))))
                .withMessageContaining("not running");

        processor.start();
        processor.stop();

        assertThatRuntimeException()
                .isThrownBy(() -> processor.process(List.of(diagnosticsAction(null))))
                .withMessageContaining("not running");
    }

    @Test
    void processReportsResultForDiagnosticsAction() {
        processor.start();

        processor.process(List.of(diagnosticsAction("{not json")));

        verify(diagnosticsService, timeout(5000))
                .addResult(argThat(result -> "a1".equals(result.id)
                        && result.status == FleetActionResult.Status.failed
                        && result.error != null));
    }

    @Test
    void processIgnoresNonDiagnosticsActions() {
        processor.start();

        processor.process(List.of(new FleetAction("a1", FleetAction.Type.NONE, FleetAction.Category.SERVER, null)));

        verifyNoInteractions(diagnosticsService);
    }
}
