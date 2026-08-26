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
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.neo4j.io.fs.FileSystemAbstraction;

class FleetDiagnosticsExecutionContextTest {

    private FleetDiagnosticsExecutionContext newContext() {
        return FleetDiagnosticsExecutionContext.create(
                Path.of("home"), Path.of("conf"), mock(FileSystemAbstraction.class));
    }

    @Test
    void redactsUrlQueryStringsFromCapturedOutput() throws IOException {
        var ctx = newContext();
        var signedUrl = "https://storage.googleapis.com/bucket-fleet-management-api-diagnostic-reports/report.zip"
                + "?X-Goog-Signature=secret";

        ctx.getOut().write(("uploading to " + signedUrl + " done").getBytes());
        ctx.getErr().write(("PUT " + signedUrl + " failed").getBytes());

        assertThat(ctx.outAsString())
                .contains(
                        "https://storage.googleapis.com/bucket-fleet-management-api-diagnostic-reports/report.zip?<redacted>")
                .doesNotContain("X-Goog-Signature");
        assertThat(ctx.errAsString()).contains("?<redacted>").doesNotContain("X-Goog-Signature");
    }

    @Test
    void leavesNonUrlOutputUntouched() throws IOException {
        var ctx = newContext();

        ctx.getOut().write("plain output, no urls".getBytes());

        assertThat(ctx.outAsString()).isEqualTo("plain output, no urls");
    }

    @Test
    void executionOutputContainsBothStreams() throws IOException {
        var ctx = newContext();

        ctx.getOut().write("some-out".getBytes());
        ctx.getErr().write("some-err".getBytes());

        assertThat(ctx.getExecutionOutput()).contains("some-out").contains("some-err");
    }
}
