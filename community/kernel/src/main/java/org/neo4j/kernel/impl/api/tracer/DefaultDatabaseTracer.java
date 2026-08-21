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
package org.neo4j.kernel.impl.api.tracer;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.transaction.log.AppendTransactionEvent;
import org.neo4j.kernel.impl.transaction.log.LogAppendEvent;
import org.neo4j.kernel.impl.transaction.log.LogFileCreateEvent;
import org.neo4j.kernel.impl.transaction.log.LogFileFlushEvent;
import org.neo4j.kernel.impl.transaction.log.LogForceEvent;
import org.neo4j.kernel.impl.transaction.log.LogForceWaitEvent;
import org.neo4j.kernel.impl.transaction.log.checkpoint.LogCheckPointEvent;
import org.neo4j.kernel.impl.transaction.log.rotation.CountingLogRotateEvent;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotateEvent;
import org.neo4j.kernel.impl.transaction.tracing.DatabaseAsyncRollbackEvent;
import org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer;
import org.neo4j.kernel.impl.transaction.tracing.RollbackBatchEvent;
import org.neo4j.kernel.impl.transaction.tracing.StoreApplyEvent;
import org.neo4j.kernel.impl.transaction.tracing.TransactionEvent;
import org.neo4j.kernel.impl.transaction.tracing.TransactionRollbackEvent;
import org.neo4j.kernel.impl.transaction.tracing.TransactionWriteEvent;

/**
 * Tracer used to trace database scoped events, like transaction logs rotations, checkpoints, transactions etc
 */
public class DefaultDatabaseTracer implements DatabaseTracer {
    private final LongAdder appendedBytes = new LongAdder();
    private final LongAdder numberOfFlushes = new LongAdder();
    private final LongAdder batchesAppended = new LongAdder();
    private final LongAdder batchesRolledBack = new LongAdder();
    private final LongAdder batchTransactionsRolledBack = new LongAdder();
    private final AtomicLong appliedBatchSize = new AtomicLong();

    private final CountingLogRotateEvent countingLogRotateEvent = new CountingLogRotateEvent();
    private final LogFileCreateEvent logFileCreateEvent = new DefaultLogFileCreateEvent();
    private final LogFileFlushEvent logFileFlushEvent = numberOfFlushes::increment;
    private final LogAppendEvent logAppendEvent = new DefaultLogAppendEvent();
    private final TransactionWriteEvent transactionWriteEvent = new DefaultTransactionWriteEvent();
    private final TransactionRollbackEvent transactionRollbackEvent = new DefaultTransactionRollbackEvent();
    private final TransactionEvent transactionEvent = new DefaultTransactionEvent();
    private final CountingLogCheckPointEvent logCheckPointEvent;

    public DefaultDatabaseTracer(PageCacheTracer pageCacheTracer) {
        this.logCheckPointEvent =
                new CountingLogCheckPointEvent(pageCacheTracer, appendedBytes, countingLogRotateEvent);
    }

    @Override
    public TransactionEvent beginTransaction(CursorContext cursorContext, long transactionSequenceNumber) {
        return transactionEvent;
    }

    @Override
    public TransactionWriteEvent beginAsyncCommit() {
        return transactionWriteEvent;
    }

    @Override
    public TransactionRollbackEvent beginAsyncTransactionRollback() {
        return transactionRollbackEvent;
    }

    @Override
    public DatabaseAsyncRollbackEvent beginAsyncDatabaseRollback() {
        return DatabaseAsyncRollbackEvent.NULL;
    }

    @Override
    public long appendedBytes() {
        return appendedBytes.longValue();
    }

    @Override
    public long numberOfLogRotations() {
        return countingLogRotateEvent.numberOfLogRotations();
    }

    @Override
    public long logRotationAccumulatedTotalTimeMillis() {
        return countingLogRotateEvent.logRotationAccumulatedTotalTimeMillis();
    }

    @Override
    public long lastLogRotationTimeMillis() {
        return countingLogRotateEvent.lastLogRotationTimeMillis();
    }

    @Override
    public long numberOfFlushes() {
        return numberOfFlushes.longValue();
    }

    @Override
    public long lastTransactionLogAppendBatch() {
        return appliedBatchSize.longValue();
    }

    @Override
    public long batchesAppended() {
        return batchesAppended.longValue();
    }

    @Override
    public long rolledbackBatches() {
        return batchesRolledBack.longValue();
    }

    @Override
    public long rolledbackBatchedTransactions() {
        return batchTransactionsRolledBack.longValue();
    }

    @Override
    public long numberOfCheckPoints() {
        return logCheckPointEvent.numberOfCheckPoints();
    }

    @Override
    public long checkPointAccumulatedTotalTimeMillis() {
        return logCheckPointEvent.checkPointAccumulatedTotalTimeMillis();
    }

    @Override
    public long lastCheckpointTimeMillis() {
        return logCheckPointEvent.lastCheckpointTimeMillis();
    }

    @Override
    public long lastCheckpointPagesFlushed() {
        return logCheckPointEvent.getPagesFlushed();
    }

    @Override
    public long lastCheckpointIOs() {
        return logCheckPointEvent.getIOsPerformed();
    }

    @Override
    public long lastCheckpointIOLimit() {
        return logCheckPointEvent.getConfiguredIOLimit();
    }

    @Override
    public long lastCheckpointIOLimitedTimes() {
        return logCheckPointEvent.getTimesPaused();
    }

    @Override
    public long lastCheckpointIOLimitedMillis() {
        return logCheckPointEvent.getMillisPaused();
    }

    @Override
    public long flushedBytes() {
        return logCheckPointEvent.flushedBytes();
    }

    @Override
    public LogCheckPointEvent beginCheckPoint() {
        return logCheckPointEvent;
    }

    @Override
    public LogFileCreateEvent createLogFile() {
        return logFileCreateEvent;
    }

    @Override
    public void openLogFile(Path filePath) {}

    @Override
    public void closeLogFile(Path filePath) {}

    @Override
    public LogAppendEvent logAppend() {
        return logAppendEvent;
    }

    @Override
    public LogFileFlushEvent flushFile() {
        return logFileFlushEvent;
    }

    public class DefaultTransactionEvent implements TransactionEvent {

        @Override
        public void setCommit(boolean commit) {}

        @Override
        public void setRollback(boolean rollback) {}

        @Override
        public TransactionWriteEvent beginCommitEvent() {
            return transactionWriteEvent;
        }

        @Override
        public TransactionWriteEvent beginChunkWriteEvent() {
            return transactionWriteEvent;
        }

        @Override
        public TransactionRollbackEvent beginRollback() {
            return transactionRollbackEvent;
        }

        @Override
        public void close() {}

        @Override
        public void setTransactionWriteState(String transactionWriteState) {}

        @Override
        public void setReadOnly(boolean wasReadOnly) {}

        @Override
        public void refreshVisibilityBoundary() {}
    }

    private class DefaultTransactionWriteEvent implements TransactionWriteEvent {
        @Override
        public void close() {}

        @Override
        public LogAppendEvent beginLogAppend() {
            return logAppendEvent;
        }

        @Override
        public StoreApplyEvent beginStoreApply() {
            return StoreApplyEvent.NULL;
        }

        @Override
        public void chunkAppended(
                int chunkNumber, long transactionSequenceNumber, long transactionId, long appendIndex) {
            batchesAppended.increment();
        }
    }

    private class DefaultTransactionRollbackEvent implements TransactionRollbackEvent {

        @Override
        public RollbackBatchEvent beginRollbackDataEvent() {
            return new RollbackBatchEvent() {
                @Override
                public void close() {
                    batchTransactionsRolledBack.increment();
                }

                @Override
                public void batchedRolledBack(long rolledBackBatches, long transactionId) {
                    batchesRolledBack.add(rolledBackBatches);
                }
            };
        }

        @Override
        public TransactionWriteEvent beginRollbackWriteEvent() {
            return transactionWriteEvent;
        }

        @Override
        public void close() {}
    }

    private class DefaultLogAppendEvent implements LogAppendEvent {
        @Override
        public void appendedBytes(long bytes) {
            assert bytes >= 0;
            appendedBytes.add(bytes);
        }

        @Override
        public void close() {}

        @Override
        public void setLogRotated(boolean logRotated) {}

        @Override
        public LogRotateEvent beginLogRotate() {
            return countingLogRotateEvent;
        }

        @Override
        public AppendTransactionEvent beginAppendTransaction(int appendItems) {
            appliedBatchSize.set(appendItems);
            return AppendTransactionEvent.NULL;
        }

        @Override
        public LogForceWaitEvent beginLogForceWait() {
            return LogForceWaitEvent.NULL;
        }

        @Override
        public LogForceEvent beginLogForce() {
            return LogForceEvent.NULL;
        }
    }

    private class DefaultLogFileCreateEvent implements LogFileCreateEvent {
        @Override
        public void fileCreated(long headerSize) {
            appendedBytes.add(headerSize);
        }

        @Override
        public void close() {}
    }
}
