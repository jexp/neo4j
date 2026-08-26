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

import static java.lang.String.format;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.regex.Pattern;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.io.fs.FileSystemAbstraction;

public class FleetDiagnosticsExecutionContext extends ExecutionContext {
    private static final Pattern HTTP_URL_PATTERN = Pattern.compile("(https?://\\S*?)\\?\\S*");

    private final ByteArrayOutputStream rawOut;
    private final ByteArrayOutputStream rawErr;

    public static FleetDiagnosticsExecutionContext create(Path homePath, Path confPath, FileSystemAbstraction fs) {
        ByteArrayOutputStream rawOut = new ByteArrayOutputStream();
        ByteArrayOutputStream rawErr = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(rawOut, true, StandardCharsets.UTF_8);
        PrintStream err = new PrintStream(rawErr, true, StandardCharsets.UTF_8);
        return new FleetDiagnosticsExecutionContext(homePath, confPath, rawOut, rawErr, out, err, fs);
    }

    private FleetDiagnosticsExecutionContext(
            Path homePath,
            Path confPath,
            ByteArrayOutputStream rawOut,
            ByteArrayOutputStream rawErr,
            PrintStream out,
            PrintStream err,
            FileSystemAbstraction fs) {
        super(homePath, confPath, out, err, fs);
        this.rawOut = rawOut;
        this.rawErr = rawErr;
    }

    public String getExecutionOutput() {
        return format("Captured System.out:%n%s%nCaptured System.err:%n%s", outAsString(), errAsString());
    }

    public OutputStream getOut() {
        return rawOut;
    }

    public OutputStream getErr() {
        return rawErr;
    }

    public String outAsString() {
        return redactUrls(rawOut.toString());
    }

    public String errAsString() {
        return redactUrls(rawErr.toString());
    }

    private static String redactUrls(String s) {
        return HTTP_URL_PATTERN.matcher(s).replaceAll("$1?<redacted>");
    }
}
