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
package org.neo4j.wal.entry.v202610;

import java.util.Objects;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.string.Mask;
import org.neo4j.wal.LogPosition;
import org.neo4j.wal.entry.AbstractDetachedCheckpointLogEntry;

public class LogEntryDetachedCheckpointV2026_10 extends AbstractDetachedCheckpointLogEntry {
    private final TransactionId transactionId;
    private final long lastAppendIndex;
    private final long consensusIndex;
    private final LogPosition oldestNotCompletedPosition;

    public LogEntryDetachedCheckpointV2026_10(
            KernelVersion kernelVersion,
            TransactionId transactionId,
            long lastAppendIndex,
            long consensusIndex,
            LogPosition oldestNotCompletedPosition,
            LogPosition checkpointedLogPosition,
            long checkpointMillis,
            StoreId storeId,
            String reason) {
        super(kernelVersion, checkpointedLogPosition, checkpointMillis, storeId, reason);
        this.transactionId = transactionId;
        this.oldestNotCompletedPosition = oldestNotCompletedPosition;
        this.lastAppendIndex = lastAppendIndex;
        this.consensusIndex = consensusIndex;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogEntryDetachedCheckpointV2026_10 that = (LogEntryDetachedCheckpointV2026_10) o;
        return lastAppendIndex == that.lastAppendIndex
                && consensusIndex == that.consensusIndex
                && checkpointTime == that.checkpointTime
                && Objects.equals(transactionId, that.transactionId)
                && Objects.equals(oldestNotCompletedPosition, that.oldestNotCompletedPosition)
                && Objects.equals(checkpointedLogPosition, that.checkpointedLogPosition)
                && Objects.equals(storeId, that.storeId)
                && Objects.equals(reason, that.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                transactionId,
                lastAppendIndex,
                consensusIndex,
                oldestNotCompletedPosition,
                checkpointedLogPosition,
                checkpointTime,
                storeId,
                reason);
    }

    public LogPosition getOldestNotCompletedPosition() {
        return oldestNotCompletedPosition;
    }

    public TransactionId getTransactionId() {
        return transactionId;
    }

    public long getLastAppendIndex() {
        return lastAppendIndex;
    }

    /**
     * @return the consensus index of the last closed batch at the time of this checkpoint, which may be different
     * from {@link #getTransactionId()}'s consensus index if the last closed batch was a rollback or a
     * not-yet-committed chunk of a bigger transaction.
     */
    public long getConsensusIndex() {
        return consensusIndex;
    }

    @Override
    public String toString(Mask mask) {
        return "LogEntryDetachedCheckpointV2026_10{" + "transactionId=" + transactionId + ", lastAppendIndex="
                + lastAppendIndex + ", consensusIndex="
                + consensusIndex + ", oldestNotCompletedPosition=" + oldestNotCompletedPosition
                + ", checkpointedLogPosition=" + checkpointedLogPosition
                + ", checkpointTime=" + checkpointTime
                + ", storeId=" + storeId + ", reason='" + reason + '}';
    }
}
