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

import static org.neo4j.storageengine.AppendIndexProvider.BASE_APPEND_INDEX;
import static org.neo4j.storageengine.api.LogVersionRepository.BASE_TX_LOG_BYTE_OFFSET;
import static org.neo4j.storageengine.api.LogVersionRepository.BASE_TX_LOG_VERSION;
import static org.neo4j.wal.EmptyLogTailMetadata.EMPTY_APPEND_BATCH_INFO;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.neo4j.io.pagecache.context.TransactionIdSnapshot;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.storageengine.api.ClosedBatchMetadata;
import org.neo4j.storageengine.api.ClosedTransactionMetadata;
import org.neo4j.storageengine.api.OpenTransactionMetadata;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.storageengine.util.HighestAppendBatch;
import org.neo4j.util.concurrent.ArrayQueueOutOfOrderSequence;
import org.neo4j.util.concurrent.OutOfOrderSequence;
import org.neo4j.util.concurrent.OutOfOrderSequence.Meta;
import org.neo4j.wal.AppendBatchInfo;
import org.neo4j.wal.LogPosition;

/**
 * Simple implementation of a {@link TransactionIdStore}.
 */
public class SimpleTransactionIdStore implements TransactionIdStore {
    private final AtomicLong committingTransactionId = new AtomicLong();
    private final OutOfOrderSequence closedTransactionId =
            new ArrayQueueOutOfOrderSequence(-1, 100, OutOfOrderSequence.EMPTY_META);
    private final OutOfOrderSequence lastClosedBatch =
            new ArrayQueueOutOfOrderSequence(-1, 100, OutOfOrderSequence.EMPTY_META);
    private final AtomicReference<TransactionId> committedTransactionId = new AtomicReference<>(BASE_TRANSACTION_ID);
    private final HighestAppendBatch appendBatchInfo = new HighestAppendBatch(EMPTY_APPEND_BATCH_INFO);
    private volatile long lowestAvailableCommittedTransactionId = TransactionIdStore.UNKNOWN_TX_ID;

    public SimpleTransactionIdStore() {
        this(
                BASE_TX_ID,
                BASE_APPEND_INDEX,
                KernelVersion.DEFAULT_BOOTSTRAP_VERSION,
                BASE_TX_CHECKSUM,
                BASE_TX_COMMIT_TIMESTAMP,
                UNKNOWN_CONSENSUS_INDEX,
                BASE_TX_LOG_VERSION,
                BASE_TX_LOG_BYTE_OFFSET);
    }

    public SimpleTransactionIdStore(
            long previouslyCommittedTxId,
            long appendIndex,
            KernelVersion kernelVersion,
            int checksum,
            long previouslyCommittedTxCommitTimestamp,
            long previousConsensusIndex,
            long previouslyCommittedTxLogVersion,
            long previouslyCommittedTxLogByteOffset) {
        assert previouslyCommittedTxId >= BASE_TX_ID : "cannot start from a tx id less than BASE_TX_ID";
        setLastCommittedAndClosedTransactionId(
                previouslyCommittedTxId,
                appendIndex,
                kernelVersion,
                checksum,
                previouslyCommittedTxCommitTimestamp,
                previousConsensusIndex,
                previouslyCommittedTxLogByteOffset,
                previouslyCommittedTxLogVersion,
                appendIndex,
                previousConsensusIndex);
    }

    @Override
    public long nextCommittingTransactionId() {
        return committingTransactionId.incrementAndGet();
    }

    @Override
    public synchronized void transactionCommitted(
            long transactionId,
            long appendIndex,
            KernelVersion kernelVersion,
            int checksum,
            long commitTimestamp,
            long consensusIndex) {
        TransactionId current = committedTransactionId.get();
        if (current == null || transactionId > current.id()) {
            committedTransactionId.set(new TransactionId(
                    transactionId, appendIndex, kernelVersion, checksum, commitTimestamp, consensusIndex));
        }
    }

    @Override
    public long getLastCommittedTransactionId() {
        return committedTransactionId.get().id();
    }

    @Override
    public TransactionId getLastCommittedTransaction() {
        return committedTransactionId.get();
    }

    @Override
    public long getHighestGapFreeClosedTransactionId() {
        return closedTransactionId.getHighestGapFreeNumber();
    }

    @Override
    public TransactionIdSnapshot getClosedTransactionSnapshot() {
        return new TransactionIdSnapshot(closedTransactionId.reverseSnapshot());
    }

    @Override
    public ClosedTransactionMetadata getHighestGapFreeClosedTransaction() {
        return new ClosedTransactionMetadata(closedTransactionId.get());
    }

    @Override
    public ClosedBatchMetadata getLastClosedBatch() {
        return new ClosedBatchMetadata(lastClosedBatch.get());
    }

    @Override
    public void setLastCommittedAndClosedTransactionId(
            long lastCommitedTxId,
            long lastClosedTxId,
            long[] notClosedTransactions,
            long transactionAppendIndex,
            KernelVersion kernelVersion,
            int checksum,
            long commitTimestamp,
            long consensusIndex,
            long byteOffset,
            long logVersion,
            long logsAppendIndex,
            long lastClosedBatchConsensusIndex,
            OpenTransactionMetadata earliestOpenTransactionMetadata,
            OutOfOrderSequence.NumberWithMeta lastClosedTxIdInfo) {
        committingTransactionId.set(lastCommitedTxId);
        committedTransactionId.set(new TransactionId(
                lastCommitedTxId, transactionAppendIndex, kernelVersion, checksum, commitTimestamp, consensusIndex));
        var txMeta = new Meta(
                logVersion,
                byteOffset,
                kernelVersion.version(),
                checksum,
                commitTimestamp,
                consensusIndex,
                transactionAppendIndex);
        var batchMeta = new Meta(
                logVersion,
                byteOffset,
                kernelVersion.version(),
                UNKNOWN_TX_CHECKSUM,
                UNKNOWN_TX_COMMIT_TIMESTAMP,
                lastClosedBatchConsensusIndex,
                logsAppendIndex);
        lastClosedBatch.set(logsAppendIndex, batchMeta);
        closedTransactionId.set(lastCommitedTxId, txMeta);
        appendBatchInfo.set(logsAppendIndex, LogPosition.UNSPECIFIED, lastClosedBatchConsensusIndex);
    }

    @Override
    public void setLastCommittedAndClosedTransactionId(
            long transactionId,
            long transactionAppendIndex,
            KernelVersion kernelVersion,
            int checksum,
            long commitTimestamp,
            long consensusIndex,
            long byteOffset,
            long logVersion,
            long appendIndex,
            long lastClosedBatchConsensusIndex) {
        committingTransactionId.set(transactionId);
        committedTransactionId.set(new TransactionId(
                transactionId, transactionAppendIndex, kernelVersion, checksum, commitTimestamp, consensusIndex));
        var txMeta = new Meta(
                logVersion,
                byteOffset,
                kernelVersion.version(),
                checksum,
                commitTimestamp,
                consensusIndex,
                transactionAppendIndex);
        var batchMeta = new Meta(
                logVersion,
                byteOffset,
                kernelVersion.version(),
                UNKNOWN_TX_CHECKSUM,
                UNKNOWN_TX_COMMIT_TIMESTAMP,
                lastClosedBatchConsensusIndex,
                appendIndex);
        lastClosedBatch.set(appendIndex, batchMeta);
        closedTransactionId.set(transactionId, txMeta);
        appendBatchInfo.set(appendIndex, LogPosition.UNSPECIFIED, lastClosedBatchConsensusIndex);
    }

    @Override
    public void transactionClosed(
            long transactionId,
            long appendIndex,
            KernelVersion kernelVersion,
            long logVersion,
            long byteOffset,
            int checksum,
            long commitTimestamp,
            long consensusIndex) {
        closedTransactionId.offer(
                transactionId,
                new Meta(
                        logVersion,
                        byteOffset,
                        kernelVersion.version(),
                        checksum,
                        commitTimestamp,
                        consensusIndex,
                        appendIndex));
    }

    @Override
    public void batchClosed(
            long transactionId,
            long appendIndex,
            boolean firstBatch,
            boolean lastBatch,
            KernelVersion kernelVersion,
            LogPosition logPositionAfter,
            long consensusIndex) {
        lastClosedBatch.offer(
                appendIndex,
                new Meta(
                        logPositionAfter.getLogVersion(),
                        logPositionAfter.getByteOffset(),
                        kernelVersion.version(),
                        UNKNOWN_TX_CHECKSUM,
                        UNKNOWN_TX_COMMIT_TIMESTAMP,
                        consensusIndex,
                        appendIndex));
    }

    @Override
    public void resetLastClosedTransaction(
            long transactionId,
            long appendIndex,
            KernelVersion kernelVersion,
            long byteOffset,
            long logVersion,
            int checksum,
            long commitTimestamp,
            long consensusIndex) {
        var meta = new Meta(
                logVersion,
                byteOffset,
                kernelVersion.version(),
                checksum,
                commitTimestamp,
                consensusIndex,
                appendIndex);
        closedTransactionId.set(transactionId, meta);
        lastClosedBatch.set(appendIndex, meta);
    }

    @Override
    public void appendBatch(
            long transactionId,
            long appendIndex,
            boolean firstBatch,
            boolean lastBatch,
            LogPosition logPositionBefore,
            LogPosition logPositionAfter,
            long consensusIndex) {
        appendBatchInfo.offer(appendIndex, logPositionAfter, consensusIndex);
    }

    @Override
    public AppendBatchInfo getLastCommittedBatch() {
        return appendBatchInfo.get();
    }

    @Override
    public OpenTransactionMetadata getOldestOpenTransaction() {
        return null;
    }

    @Override
    public TransactionId getHighestEverClosedTransaction() {
        return committedTransactionId.get();
    }

    @Override
    public void setLowestAvailableCommittedTransactionId(long transactionId) {
        this.lowestAvailableCommittedTransactionId = transactionId;
    }

    @Override
    public long getLowestAvailableCommittedTransactionId() {
        return lowestAvailableCommittedTransactionId;
    }
}
