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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.fleetmanagement.actions.FleetAction;
import org.neo4j.fleetmanagement.actions.FleetActionResult;
import org.neo4j.fleetmanagement.utils.Logger;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.NullLogService;

class GenerateAndUploadDiagnosticsReportTest {

    private final GenerateAndUploadDiagnosticsReport handler =
            new GenerateAndUploadDiagnosticsReport(NullLogService.getInstance(), null, null, null, null);

    @BeforeAll
    static void beforeAll() {
        Logger.initLogger(mock(Log.class));
    }

    private static FleetAction actionWithPayload(String payload) {
        return new FleetAction(
                "a1", FleetAction.Type.GENERATE_AND_UPLOAD_DIAGNOSTICS_REPORT, FleetAction.Category.SERVER, payload);
    }

    @Test
    void executeWithMalformedPayloadReturnsFailedResult() {
        var result = handler.execute(actionWithPayload("{not json"));

        assertThat(result.id).isEqualTo("a1");
        assertThat(result.status).isEqualTo(FleetActionResult.Status.failed);
        assertThat(result.error).isNotBlank();
    }

    @Test
    void uploadRejectsUrlsOutsideTheGcsDiagnosticsBucket() {
        var ctx = FleetDiagnosticsExecutionContext.create(
                Path.of("home"), Path.of("conf"), mock(FileSystemAbstraction.class));
        var file = new File("report.zip");
        var result = new FleetActionResult("a1", FleetActionResult.Status.running, null, null);

        assertThatIllegalArgumentException()
                .isThrownBy(() -> handler.upload(ctx, "https://example.com/report.zip?sig=abc", file, result));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> handler.upload(
                        ctx,
                        "http://storage.googleapis.com/abc-fleet-management-api-diagnostic-reports/report.zip?sig=abc",
                        file,
                        result));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> handler.upload(
                        ctx, "https://storage.googleapis.com/some-other-bucket/report.zip?sig=abc", file, result));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> handler.upload(
                        ctx,
                        "https://storage.googleapis.com/abc-fleet-management-api-diagnostic-reports/report.zip",
                        file,
                        result));
    }
}
