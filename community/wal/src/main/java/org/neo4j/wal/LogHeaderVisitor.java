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
package org.neo4j.wal;

import org.neo4j.wal.entry.LogHeader;

public interface LogHeaderVisitor {
    /***
     * Used for visiting log headers in reverse order of age, meaning latest first.
     * Stops visiting when false is returned.
     *
     * NOTE that firstAppendIndexInLog is not necessarily in the logfile - it is based on the last append index from
     * the previous log file and a guess that the last chunk from that file isn't spanning the whole of the current file
     * too. firstAppendIndexInLog is only guaranteed to be in the log file if firstAppendIndexInLog <= lastAppendIndexInLog
     * @return {@code true} for continue visiting log headers, otherwise {@code false} for breaking after this header.
     */
    boolean visit(LogHeader logHeader, LogPosition position, long firstAppendIndexInLog, long lastAppendIndexInLog);
}
