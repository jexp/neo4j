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
package org.neo4j.wal.entry.v520;

import static org.neo4j.wal.entry.LogEntryTypeCodes.TX_ROLLBACK;

import java.io.IOException;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.wal.LogPositionMarker;
import org.neo4j.wal.entry.LogEntrySerializer;
import org.neo4j.wal.entry.LogEntryTypeCodes;

public class RollbackLogEntrySerializerV5_20 extends LogEntrySerializer<LogEntryRollback> {

    public RollbackLogEntrySerializerV5_20() {
        super(LogEntryTypeCodes.TX_ROLLBACK);
    }

    @Override
    public LogEntryRollback parse(
            KernelVersion version,
            ReadableChannel channel,
            LogPositionMarker marker,
            CommandReaderFactory commandReaderFactory,
            MemoryTracker memoryTracker)
            throws IOException {
        long transactionId = channel.getLong();
        long timeWritten = channel.getLong();
        long chunkId = channel.getLong();
        long transactionSequenceNumber = channel.getLong();
        long lastBatchAppendIndex = channel.getLong();
        long appendIndex = channel.getAppendIndex();
        int checksum = channel.endChecksumAndValidate();
        return new LogEntryRollback(
                version,
                transactionId,
                appendIndex,
                chunkId,
                timeWritten,
                checksum,
                transactionSequenceNumber,
                lastBatchAppendIndex);
    }

    @Override
    public int write(WritableChannel channel, LogEntryRollback logEntry) throws IOException {
        channel.beginChecksumForWriting();
        writeLogEntryHeader(logEntry.kernelVersion(), TX_ROLLBACK, channel);
        channel.putLong(logEntry.getTransactionId())
                .putLong(logEntry.getTimeWritten())
                .putLong(logEntry.getChunkId())
                .putLong(logEntry.getTransactionSequenceNumber())
                .putLong(logEntry.getLastBatchAppendIndex())
                .putAppendIndex(logEntry.getAppendIndex());
        return channel.putChecksum();
    }
}
