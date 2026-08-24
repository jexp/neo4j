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

import java.nio.file.Path;

public interface LogTracers extends TransactionLogCounters {
    LogTracers NULL = new LogTracers() {
        @Override
        public LogFileCreateEvent createLogFile() {
            return LogFileCreateEvent.NULL;
        }

        @Override
        public void openLogFile(Path filePath) {}

        @Override
        public void closeLogFile(Path filePath) {}

        @Override
        public LogAppendEvent logAppend() {
            return LogAppendEvent.NULL;
        }

        @Override
        public LogFileFlushEvent flushFile() {
            return LogFileFlushEvent.NULL;
        }

        @Override
        public long appendedBytes() {
            return 0;
        }

        @Override
        public long numberOfLogRotations() {
            return 0;
        }

        @Override
        public long logRotationAccumulatedTotalTimeMillis() {
            return 0;
        }

        @Override
        public long lastLogRotationTimeMillis() {
            return 0;
        }

        @Override
        public long numberOfFlushes() {
            return 0;
        }

        @Override
        public long lastTransactionLogAppendBatch() {
            return 0;
        }

        @Override
        public long batchesAppended() {
            return 0;
        }

        @Override
        public long rolledbackBatches() {
            return 0;
        }

        @Override
        public long rolledbackBatchedTransactions() {
            return 0;
        }
    };

    LogFileCreateEvent createLogFile();

    void openLogFile(Path filePath);

    void closeLogFile(Path filePath);

    LogAppendEvent logAppend();

    LogFileFlushEvent flushFile();
}
