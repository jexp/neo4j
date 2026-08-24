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
package org.neo4j.kernel.impl.transaction;

import static org.neo4j.storageengine.AppendIndexProvider.UNKNOWN_APPEND_INDEX;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_TX_CHECKSUM;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_TX_SEQUENCE_NUMBER;

import java.io.IOException;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.storageengine.api.CommandBatch;
import org.neo4j.wal.EmptyCommandBatch;
import org.neo4j.wal.entry.LogEntryWriter;

public record EmptyBatchRepresentation(KernelVersion kernelVersion, long appendIndex)
        implements CommittedCommandBatchRepresentation {

    @Override
    public CommandBatch commandBatch() {
        return new EmptyCommandBatch(kernelVersion, appendIndex);
    }

    @Override
    public int serialize(LogEntryWriter<? extends WritableChannel> writer) throws IOException {
        throw new IllegalStateException("EmptyBatchRepresentation should never be serialized");
    }

    @Override
    public int checksum() {
        return UNKNOWN_TX_CHECKSUM;
    }

    @Override
    public int previousChecksum() {
        return UNKNOWN_TX_CHECKSUM;
    }

    @Override
    public long timeWritten() {
        return -1;
    }

    @Override
    public long txId() {
        return appendIndex;
    }

    @Override
    public boolean isRollback() {
        return false;
    }

    @Override
    public long transactionSequenceNumber() {
        return UNKNOWN_TX_SEQUENCE_NUMBER;
    }

    @Override
    public long previousBatchAppendIndex() {
        return UNKNOWN_APPEND_INDEX;
    }
}
