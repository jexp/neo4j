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

import org.neo4j.kernel.KernelVersion;
import org.neo4j.string.Mask;
import org.neo4j.wal.entry.AbstractVersionAwareLogEntry;

public class LogEntryRollback extends AbstractVersionAwareLogEntry {
    private final long transactionId;
    private final long timeWritten;
    private final int checksum;
    private final long appendIndex;
    private final long chunkId;
    private final long transactionSequenceNumber;
    private final long lastBatchAppendIndex;

    public LogEntryRollback(
            KernelVersion kernelVersion,
            long transactionId,
            long appendIndex,
            long chunkId,
            long timeWritten,
            int checksum,
            long transactionSequenceNumber,
            long lastBatchAppendIndex) {
        super(kernelVersion, TX_ROLLBACK);
        this.transactionId = transactionId;
        this.timeWritten = timeWritten;
        this.checksum = checksum;
        this.appendIndex = appendIndex;
        this.chunkId = chunkId;
        this.transactionSequenceNumber = transactionSequenceNumber;
        this.lastBatchAppendIndex = lastBatchAppendIndex;
    }

    public long getTransactionId() {
        return transactionId;
    }

    public long getTimeWritten() {
        return timeWritten;
    }

    public int getChecksum() {
        return checksum;
    }

    public long getAppendIndex() {
        return appendIndex;
    }

    public long getChunkId() {
        return chunkId;
    }

    public long getTransactionSequenceNumber() {
        return transactionSequenceNumber;
    }

    public long getLastBatchAppendIndex() {
        return lastBatchAppendIndex;
    }

    @Override
    public String toString(Mask mask) {
        return "LogEntryRollbackV5_20{" + "txId="
                + transactionId + ", timeWritten="
                + timeWritten + ", checksum="
                + checksum + ", appendIndex=" + appendIndex + ", chunkId=" + chunkId
                + ", transactionSequenceNumber=" + transactionSequenceNumber
                + ", lastBatchAppendIndex=" + lastBatchAppendIndex + '}';
    }
}
