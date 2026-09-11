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

import static org.neo4j.kernel.KernelVersion.VERSION_APPEND_INDEX_INTRODUCED;
import static org.neo4j.storageengine.AppendIndexProvider.UNKNOWN_APPEND_INDEX;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;
import static org.neo4j.wal.LogIndexEncoding.decodeLogIndex;

import java.io.IOException;
import org.neo4j.wal.entry.LogEntry;
import org.neo4j.wal.entry.LogEntryEmpty;
import org.neo4j.wal.entry.LogEntryReader;
import org.neo4j.wal.entry.LogEntryStart;
import org.neo4j.wal.entry.v520.LogEntryChunkStart;
import org.neo4j.wal.entry.v520.LogEntryRollback;

/**
 * Locates the consensus index of the batch appended at a given append index. Resolves to
 * {@link org.neo4j.storageengine.api.TransactionIdStore#UNKNOWN_CONSENSUS_INDEX} when the batch carries no consensus
 * index - an empty transaction, an entry written by a standalone database, or one from before
 * {@link org.neo4j.kernel.KernelVersion#VERSION_APPEND_INDEX_INTRODUCED} - and when the batch isn't in the log at all.
 */
public class AppendedChunkConsensusIndexLocator implements LogFile.LogFileVisitor {
    private final long appendIndex;
    private final LogEntryReader logEntryReader;
    private long consensusIndex = UNKNOWN_CONSENSUS_INDEX;

    public AppendedChunkConsensusIndexLocator(long appendIndex, LogEntryReader logEntryReader) {
        this.appendIndex = appendIndex;
        this.logEntryReader = logEntryReader;
    }

    @Override
    public boolean visit(ReadableLogPositionAwareChannel channel) throws IOException {
        channel.alignWithStartEntry();

        while (true) {
            LogEntry logEntry = logEntryReader.readLogEntry(channel);
            if (logEntry == null) {
                // Reached end without finding the batch
                return true;
            }

            long entryAppendIndex = appendIndexOf(logEntry);
            if (entryAppendIndex == UNKNOWN_APPEND_INDEX || entryAppendIndex < appendIndex) {
                continue;
            }
            if (entryAppendIndex == appendIndex) {
                consensusIndex = consensusIndexOf(logEntry);
            }
            // Either found it, or scanned past it, in which case it isn't in the log
            return false;
        }
    }

    public long getConsensusIndex() {
        return consensusIndex;
    }

    private static long appendIndexOf(LogEntry logEntry) {
        return switch (logEntry) {
            case LogEntryStart start
            when start.kernelVersion().isAtLeast(VERSION_APPEND_INDEX_INTRODUCED) -> start.getAppendIndex();
            case LogEntryChunkStart chunkStart -> chunkStart.getAppendIndex();
            case LogEntryRollback rollback -> rollback.getAppendIndex();
            case LogEntryEmpty empty -> empty.getAppendIndex();
            default -> UNKNOWN_APPEND_INDEX;
        };
    }

    private static long consensusIndexOf(LogEntry logEntry) {
        return switch (logEntry) {
            case LogEntryStart start -> decodeConsensusIndex(start.getAdditionalHeader());
            case LogEntryChunkStart chunkStart -> decodeConsensusIndex(chunkStart.getAdditionalHeader());
            case LogEntryRollback rollback -> rollback.getConsensusIndex();
            default -> UNKNOWN_CONSENSUS_INDEX;
        };
    }

    private static long decodeConsensusIndex(byte[] additionalHeader) {
        try {
            return decodeLogIndex(additionalHeader);
        } catch (IllegalArgumentException e) {
            return UNKNOWN_CONSENSUS_INDEX;
        }
    }
}
