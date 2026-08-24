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
package org.neo4j.wal.reverse;

import static org.neo4j.wal.LogVersionBridge.NO_MORE_CHANNELS;
import static org.neo4j.wal.reverse.EagerlyReversedCommandBatchCursor.eagerlyReverse;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import org.neo4j.wal.CommandBatchCursor;
import org.neo4j.wal.CommittedCommandBatchCursor;
import org.neo4j.wal.LogFile;
import org.neo4j.wal.LogPosition;
import org.neo4j.wal.ReadAheadLogChannel;
import org.neo4j.wal.ReadableLogChannel;
import org.neo4j.wal.entry.LogEntryReader;

public class PrefetchedReverseCommandBatchCursors implements CommandBatchCursors {
    private final BlockingQueue<CommandBatchCursor> cursors = new LinkedBlockingDeque<>(2);
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final LogFile logFile;
    private final LogPosition beginning;
    private final LogEntryReader reader;
    private final boolean failOnCorruptedLogFiles;
    private final ReversedTransactionCursorMonitor monitor;
    private long currentVersion;
    private final LogPosition maxPosition;

    public PrefetchedReverseCommandBatchCursors(
            LogFile logFile,
            LogPosition beginning,
            LogEntryReader reader,
            boolean failOnCorruptedLogFiles,
            ReversedTransactionCursorMonitor monitor,
            LogPosition maxPosition) {
        this.logFile = logFile;
        this.beginning = beginning;
        this.reader = reader;
        this.failOnCorruptedLogFiles = failOnCorruptedLogFiles;
        this.monitor = monitor;
        this.currentVersion = logFile.getLogRangeInfo().highestVersion();
        this.maxPosition = maxPosition;
        monitor.presketchingTransactionLogs();
        executor.execute(this::prepare);
    }

    @Override
    public Optional<CommandBatchCursor> next() {
        try {
            var cursor = cursors.take();
            if (cursor == NO_MORE_CURSORS) {
                return Optional.empty();
            }

            return Optional.of(cursor);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void prepare() {
        try {
            while (currentVersion >= beginning.getLogVersion()) {
                LogPosition position = currentVersion > beginning.getLogVersion()
                        ? logFile.extractHeader(currentVersion).getStartPosition()
                        : beginning;
                ReadableLogChannel channel = logFile.getReader(position, NO_MORE_CHANNELS);
                if (channel instanceof ReadAheadLogChannel readAheadLogChannel) {
                    cursors.put(ReversedSingleFileCommandBatchCursor.create(
                            readAheadLogChannel, reader, failOnCorruptedLogFiles, monitor, maxPosition));
                } else {
                    cursors.put(eagerlyReverse(new CommittedCommandBatchCursor(channel, reader, maxPosition)));
                }
                currentVersion--;
            }

            // Poison pill
            cursors.put(NO_MORE_CURSORS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        executor.shutdown();
    }
}
