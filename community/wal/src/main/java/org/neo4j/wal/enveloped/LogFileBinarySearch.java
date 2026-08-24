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
package org.neo4j.wal.enveloped;

import static org.neo4j.wal.entry.LogHeaderReader.readLogHeader;

import java.io.IOException;
import org.neo4j.memory.MemoryTracker;

class LogFileBinarySearch implements LogBinarySearch.BinarySearchReader {
    private final LogsRepository logsRepository;
    private final int numFiles;
    private final long startFileVersion;
    private final MemoryTracker memoryTracker;

    public LogFileBinarySearch(
            LogsRepository logsRepository, long startFileVersion, long numFiles, MemoryTracker memoryTracker) {
        this.logsRepository = logsRepository;
        this.numFiles = (int) numFiles;
        this.startFileVersion = startFileVersion;
        this.memoryTracker = memoryTracker;
    }

    @Override
    public int size() {
        return numFiles;
    }

    @Override
    public long get(int index) {
        return startFileVersion + index;
    }

    @Override
    public int compare(long version, long targetEntryIndex) {
        try (var logChannelContext = logsRepository.openReadChannel(version)) {

            var logHeader = readLogHeader(logChannelContext.channel(), true, null, memoryTracker);
            if (logHeader != null) {
                var lastAppendIndex = logHeader.getLastAppendIndex();
                if (lastAppendIndex < targetEntryIndex) {
                    return -1;
                } else {
                    return 1;
                }
            }
            return 1;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
