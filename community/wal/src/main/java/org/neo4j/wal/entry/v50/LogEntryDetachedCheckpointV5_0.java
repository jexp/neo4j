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
package org.neo4j.wal.entry.v50;

import java.util.Objects;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.string.Mask;
import org.neo4j.wal.LogPosition;
import org.neo4j.wal.entry.AbstractDetachedCheckpointLogEntry;

public class LogEntryDetachedCheckpointV5_0 extends AbstractDetachedCheckpointLogEntry {
    private final TransactionId transactionId;
    private final boolean consensusIndexInCheckpoint;

    public LogEntryDetachedCheckpointV5_0(
            KernelVersion kernelVersion,
            TransactionId transactionId,
            LogPosition checkpointedLogPosition,
            long checkpointMillis,
            StoreId storeId,
            String reason) {
        this(kernelVersion, transactionId, checkpointedLogPosition, checkpointMillis, storeId, reason, true);
    }

    public LogEntryDetachedCheckpointV5_0(
            KernelVersion kernelVersion,
            TransactionId transactionId,
            LogPosition logPosition,
            long checkpointMillis,
            StoreId storeId,
            String reason,
            boolean consensusIndexInCheckpoint) {
        super(kernelVersion, logPosition, checkpointMillis, storeId, reason);
        this.transactionId = transactionId;
        this.consensusIndexInCheckpoint = consensusIndexInCheckpoint;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogEntryDetachedCheckpointV5_0 that = (LogEntryDetachedCheckpointV5_0) o;
        return checkpointTime == that.checkpointTime
                && Objects.equals(transactionId, that.transactionId)
                && Objects.equals(checkpointedLogPosition, that.checkpointedLogPosition)
                && Objects.equals(storeId, that.storeId)
                && kernelVersion() == that.kernelVersion()
                && Objects.equals(reason, that.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kernelVersion(), transactionId, checkpointedLogPosition, checkpointTime, storeId, reason);
    }

    public TransactionId getTransactionId() {
        return transactionId;
    }

    public boolean consensusIndexInCheckpoint() {
        return consensusIndexInCheckpoint;
    }

    @Override
    public String toString(Mask mask) {
        return "LogEntryDetachedCheckpointV5_0{" + "transactionId="
                + transactionId + ", logPosition="
                + checkpointedLogPosition + ", checkpointTime="
                + checkpointTime + ", storeId="
                + storeId + ", reason='"
                + reason + '\'' + ", version="
                + kernelVersion() + '}';
    }
}
