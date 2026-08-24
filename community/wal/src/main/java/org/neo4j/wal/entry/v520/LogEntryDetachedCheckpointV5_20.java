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

import java.util.Objects;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.string.Mask;
import org.neo4j.wal.LogPosition;
import org.neo4j.wal.entry.AbstractDetachedCheckpointLogEntry;

public class LogEntryDetachedCheckpointV5_20 extends AbstractDetachedCheckpointLogEntry {
    private final TransactionId transactionId;
    private final long lastAppendIndex;
    private final boolean consensusIndexInCheckpoint;

    public LogEntryDetachedCheckpointV5_20(
            KernelVersion kernelVersion,
            TransactionId transactionId,
            long lastAppendIndex,
            LogPosition checkpointedLogPosition,
            long checkpointMillis,
            StoreId storeId,
            String reason) {
        super(kernelVersion, checkpointedLogPosition, checkpointMillis, storeId, reason);
        this.transactionId = transactionId;
        this.lastAppendIndex = lastAppendIndex;
        this.consensusIndexInCheckpoint = true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogEntryDetachedCheckpointV5_20 that = (LogEntryDetachedCheckpointV5_20) o;
        return lastAppendIndex == that.lastAppendIndex
                && checkpointTime == that.checkpointTime
                && consensusIndexInCheckpoint == that.consensusIndexInCheckpoint
                && Objects.equals(transactionId, that.transactionId)
                && Objects.equals(checkpointedLogPosition, that.checkpointedLogPosition)
                && Objects.equals(storeId, that.storeId)
                && Objects.equals(reason, that.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                transactionId,
                lastAppendIndex,
                checkpointedLogPosition,
                checkpointTime,
                storeId,
                reason,
                consensusIndexInCheckpoint);
    }

    public TransactionId getTransactionId() {
        return transactionId;
    }

    public boolean consensusIndexInCheckpoint() {
        return consensusIndexInCheckpoint;
    }

    public long getLastAppendIndex() {
        return lastAppendIndex;
    }

    @Override
    public String toString(Mask mask) {
        return "LogEntryDetachedCheckpointV5_20{" + "transactionId=" + transactionId + ", lastAppendIndex="
                + lastAppendIndex + ", logPosition="
                + checkpointedLogPosition + ", checkpointTime=" + checkpointTime + ", storeId=" + storeId + ", reason='"
                + reason + "', consensusIndexInCheckpoint=" + consensusIndexInCheckpoint + '}';
    }
}
