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

import static org.apache.commons.lang3.ArrayUtils.EMPTY_LONG_ARRAY;

import java.io.IOException;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;
import org.neo4j.io.fs.ReadPastEndException;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatchRepresentation;
import org.neo4j.wal.CommandBatchCursor;
import org.neo4j.wal.CommittedCommandBatchCursor;
import org.neo4j.wal.LogPosition;
import org.neo4j.wal.entry.LogEntryReader;
import org.neo4j.wal.entry.UnsupportedLogVersionException;
import org.neo4j.wal.enveloped.EnvelopeReadChannel;
import org.neo4j.wal.enveloped.IncompleteEnvelopeReadException;
import org.neo4j.wal.enveloped.InvalidEndOfFileReadException;

/**
 * Returns command batches in reverse order in an enveloped log file. It tries to keep peak memory consumption to a minimum
 * by first sketching out the offsets of all transactions in the log. Then it starts from the end and moves backwards.
 * Since a transaction can stretch over multiple files this cursor has to handle having read to subsequent files when
 * jumping backwards.
 *
 * This cursor handles all transactions beginning in the sent in file, potentially reading over several files to
 * assemble the last transaction seen (first returned).
 */
public class ReversedEnvelopedCommandBatchCursor implements CommandBatchCursor {
    private EnvelopeReadChannel currentChannel;
    private final boolean failOnCorruptedLogFiles;
    private final ReversedTransactionCursorMonitor monitor;
    private EnvelopeReadChannel unbridgedChannel;
    private final LogEntryReader logEntryReader;
    private CommandBatchCursor commandBatchCursor;
    private final long logVersion;
    private CommittedCommandBatchRepresentation currentCommandBatch;
    private final LongIterator offsets;
    private LogPosition currentBatchStartPosition;

    ReversedEnvelopedCommandBatchCursor(
            EnvelopeReadChannel channel,
            LogEntryReader logEntryReader,
            boolean failOnCorruptedLogFiles,
            ReversedTransactionCursorMonitor monitor,
            EnvelopeReadChannel bridgedChannel,
            LogPosition maxPosition)
            throws IOException {
        this.unbridgedChannel = channel;
        this.currentChannel = bridgedChannel;
        this.failOnCorruptedLogFiles = failOnCorruptedLogFiles;
        boolean failOnLastBrokenEntry = maxPosition != LogPosition.UNSPECIFIED;
        // Make sure we always use failOnCorruptedLogFiles when we have a maxOffset
        assert (failOnLastBrokenEntry && failOnCorruptedLogFiles) || !failOnLastBrokenEntry
                : "Fail on broken/corrupted not as expected. failOnLastBrokenEntry %s, failOnCorruptedLogFiles %s"
                        .formatted(failOnLastBrokenEntry, failOnCorruptedLogFiles);
        this.monitor = monitor;
        this.commandBatchCursor = new CommittedCommandBatchCursor(bridgedChannel, logEntryReader);
        this.logVersion = channel.getLogVersion();
        this.offsets = sketchOutEnvelopeStartOffsets(maxPosition);
        this.logEntryReader = logEntryReader;
    }

    private LongIterator sketchOutEnvelopeStartOffsets(LogPosition maxPosition) throws IOException {
        LongArrayList entryStartPositions = new LongArrayList(10_000);

        long maxOffset = Long.MAX_VALUE;
        if (maxPosition != LogPosition.UNSPECIFIED) {
            if (maxPosition.getLogVersion() == logVersion) {
                maxOffset = maxPosition.getByteOffset();
                if (unbridgedChannel.position() > maxOffset) {
                    throw new IllegalArgumentException("Already read past max position %d, currently at %d"
                            .formatted(maxOffset, unbridgedChannel.position()));
                }

                if (unbridgedChannel.position() == maxOffset) {
                    // Nothing interesting in this file at all
                    return entryStartPositions.longIterator();
                }
            }
            assert maxPosition.getLogVersion() >= logVersion
                    : "ReversedEnvelopedCommandBatchCursor should not be created for file (%d) above maxPosition %s"
                            .formatted(logVersion, maxPosition);
        }

        try {
            // We don't want to go to next entry because that might be passed our maxOffset if
            // there are no transactions of interest in this file.
            // But we know we should at least look at the first envelope because we are not already at maxOffset.
            long pos = unbridgedChannel.goToNextEnvelope();
            if (unbridgedChannel.isStartEnvelope()) {
                entryStartPositions.add(pos);
            }
            long prevPos = pos;

            while ((pos = unbridgedChannel.goToEndOfEntry().getByteOffset()) < maxOffset && pos > prevPos) {
                pos = unbridgedChannel.goToNextEntry();
                entryStartPositions.add(pos);
                prevPos = pos;
            }

            if (maxOffset == Long.MAX_VALUE) {
                // Should have read to the end of the file.. Something is wrong
                throw new IOException("Failed to read to end of log file version %d. Last seen byte offset %d"
                        .formatted(logVersion, pos));
            }
            if (pos != maxOffset) {
                throw new IllegalStateException(
                        "Log file ended (at %s) before requested maxOffset %s".formatted(pos, maxOffset));
            }
        } catch (ReadPastEndException | InvalidEndOfFileReadException | IncompleteEnvelopeReadException e) {
            // Expected
            // Or well, IncompleteEnvelopeReadException isn't really expected, but it is signaling that the last
            // entry is broken. Last broken entry is not considered a corrupted log, and is recoverable.
            // The logs will be truncated after forward recovery.
            if (maxOffset != Long.MAX_VALUE) {
                throw new IllegalStateException(
                        e instanceof ReadPastEndException
                                ? "Log file ended before (at %s) before requested maxOffset %s"
                                        .formatted(unbridgedChannel.position(), maxOffset)
                                : "Log file ends with a last broken entry which is not okay when recovering to a specific position."
                                        + " All broken parts should already have been truncated at this point.");
            }
        } catch (IOException | RuntimeException e) {
            boolean first = entryStartPositions.isEmpty();
            monitor.transactionalLogRecordReadFailure(
                    first ? EMPTY_LONG_ARRAY : new long[] {entryStartPositions.getLast()}, first ? 0 : 1, logVersion);
            if (failOnCorruptedLogFiles) {
                // we fail to sketch out offsets and no one will close this cursor since construction was never
                // completed
                currentChannel.close();
                throw e;
            }
        }

        if (unbridgedChannel.getLogVersion() != logVersion) {
            throw new IllegalArgumentException(
                    "The channel which was passed in bridged multiple log versions, it started at version " + logVersion
                            + ", but continued through to version " + unbridgedChannel.getLogVersion()
                            + ". This isn't supported");
        }

        return entryStartPositions.asReversed().longIterator();
    }

    @Override
    public boolean next() throws IOException {
        if (!offsets.hasNext()) {
            return false;
        }

        // If a tx split over several files was read, a new cursor starting on the correct file is needed.
        if (commandBatchCursor.position().getLogVersion() != logVersion) {
            resetToUnbridgedBatchCursor();
        }
        long next = offsets.next();
        try {
            currentChannel.setPositionUnsafe(next);
            if (!commandBatchCursor.next()) {
                resetToUnbridgedBatchCursor();
                // For a last broken entry it could have gotten the offset but then later found incomplete envelopes.
                // Need to continue to the previous one
                return next();
            }
        } catch (IllegalStateException | IOException | UnsupportedLogVersionException e) {
            // Since the content is never read while sketching out the offsets any corruption can be found at this point
            // This means batches after the corruption can already have been returned, but that should be fine since we
            // are in reverse recovery
            monitor.transactionalLogRecordReadFailure(new long[] {next}, 1, logVersion);
            if (failOnCorruptedLogFiles) {
                throw e;
            }
            resetToUnbridgedBatchCursor();
            return next();
        }
        currentCommandBatch = commandBatchCursor.get();
        currentBatchStartPosition = new LogPosition(logVersion, next);
        return true;
    }

    private void resetToUnbridgedBatchCursor() throws IOException {
        if (unbridgedChannel != null) {
            commandBatchCursor.close();
            currentChannel = unbridgedChannel;
            unbridgedChannel = null;
            commandBatchCursor = new CommittedCommandBatchCursor(currentChannel, logEntryReader);
        }
    }

    @Override
    public void close() throws IOException {
        commandBatchCursor.close(); // closes the channel too
        if (unbridgedChannel != null) {
            unbridgedChannel.close();
        }
    }

    @Override
    public CommittedCommandBatchRepresentation get() {
        return currentCommandBatch;
    }

    @Override
    public LogPosition position() {
        assert currentBatchStartPosition != null;
        return currentBatchStartPosition;
    }
}
