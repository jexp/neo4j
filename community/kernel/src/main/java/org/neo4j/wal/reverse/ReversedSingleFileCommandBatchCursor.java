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
import static org.neo4j.util.Preconditions.checkState;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import org.neo4j.io.fs.ReadAheadChannel;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatchRepresentation;
import org.neo4j.wal.CommandBatchCursor;
import org.neo4j.wal.CommittedCommandBatchCursor;
import org.neo4j.wal.LogPosition;
import org.neo4j.wal.LogVersionBridge;
import org.neo4j.wal.ReadAheadLogChannel;
import org.neo4j.wal.SketchingCommandBatchCursor;
import org.neo4j.wal.entry.LogEntryReader;
import org.neo4j.wal.entry.UnsupportedLogVersionException;

/**
 * Returns command batches in reverse order in a log file. It tries to keep peak memory consumption to a minimum
 * by first sketching out the offsets of all transactions in the log. Then it starts from the end and moves backwards,
 * taking advantage of read-ahead feature of the {@link ReadAheadLogChannel} by moving in chunks backwards in roughly
 * the size of the read-ahead window. Coming across large batches means moving further back to at least read one batch
 * per chunk "move". This is all internal, so from the outside it simply reverses a transaction log.
 * The memory overhead compared to reading a log in the natural order is almost negligible.
 *
 * This cursor currently only works for a single log file, such that the given {@link ReadAheadLogChannel} should not be
 * instantiated with a {@link LogVersionBridge} moving it over to other versions when exhausted. For reversing a whole
 * log stream consisting of multiple log files have a look at {@link ReversedMultiFileCommandBatchCursor}.
 *
 * <pre>
 *
 *              ◄────────────────┤                          {@link #chunkBatches} for the current chunk, reading {@link #readNextChunk()}.
 * [2  |3|4    |5  |6          |7 |8   |9      |10  ]
 * ▲   ▲ ▲     ▲   ▲           ▲  ▲    ▲       ▲
 * │   │ │     │   │           │  │    │       │
 * └───┴─┴─────┼───┴───────────┴──┴────┴───────┴─────────── {@link #offsets}
 *             │
 *             └─────────────────────────────────────────── {@link #chunkStartOffsetIndex} moves forward in {@link #readNextChunk()}
 *
 * </pre>
 *
 * @see ReversedMultiFileCommandBatchCursor
 */
public class ReversedSingleFileCommandBatchCursor implements CommandBatchCursor {
    // Should this be passed in or extracted from the read-ahead channel instead?
    private static final int CHUNK_SIZE = ReadAheadChannel.DEFAULT_READ_AHEAD_SIZE;

    private final ReadAheadLogChannel channel;
    private final CommandBatchCursor commandBatchCursor;
    // Should be generally large enough to hold transactions in a chunk, where one chunk is the read-ahead size of
    // ReadAheadLogChannel
    private final Deque<ReservedBatch> chunkBatches = new ArrayDeque<>(20);

    private CommittedCommandBatchRepresentation currentCommandBatch;
    private LogPosition currentBatchStartPosition;

    // May be longer than required, offsetLength holds the actual length.
    private final long[] offsets;
    private final int offsetsLength;
    private final long totalSize;
    private int chunkStartOffsetIndex;

    static ReversedSingleFileCommandBatchCursor create(
            ReadAheadLogChannel channel,
            LogEntryReader logEntryReader,
            boolean failOnCorruptedLogFiles,
            ReversedTransactionCursorMonitor monitor,
            LogPosition maxPosition)
            throws IOException {

        boolean failOnLastBrokenEntry = maxPosition != LogPosition.UNSPECIFIED;

        // Make sure we always use failOnCorruptedLogFiles when we have a maxPosition
        assert (failOnLastBrokenEntry && failOnCorruptedLogFiles) || !failOnLastBrokenEntry;

        long logVersion = channel.getLogVersion();
        long startOffset = channel.position();

        long maxOffset = Long.MAX_VALUE;
        if (maxPosition != LogPosition.UNSPECIFIED && maxPosition.getLogVersion() == logVersion) {
            maxOffset = maxPosition.getByteOffset();
            if (startOffset > maxOffset) {
                throw new IllegalArgumentException(
                        "Already read past max position %d, currently at %d".formatted(maxOffset, startOffset));
            }
            if (startOffset == maxOffset) {
                // Nothing interesting in this file at all
                return new ReversedSingleFileCommandBatchCursor(
                        channel, logEntryReader, EMPTY_LONG_ARRAY, 0, startOffset);
            }
        }

        long[] offsets = new long[10_000];
        int offsetCursor = 0;

        SketchingCommandBatchCursor sketchingCursor = new SketchingCommandBatchCursor(channel, logEntryReader);

        try {
            // TODO Can we have a case where nothing should be read in this file?
            while (sketchingCursor.next()) {
                if (offsetCursor == offsets.length) {
                    offsets = Arrays.copyOf(offsets, offsetCursor * 2);
                }
                offsets[offsetCursor++] = startOffset;
                startOffset = channel.position();
                if (startOffset >= maxOffset) {
                    checkState(
                            startOffset == maxOffset,
                            "Max position must align with transaction start. max: %s, current offset: %d",
                            maxPosition,
                            startOffset);
                    // Read up to the requested position, let's stop
                    break;
                }
            }
        } catch (IllegalStateException | IOException | UnsupportedLogVersionException e) {
            monitor.transactionalLogRecordReadFailure(offsets, offsetCursor, logVersion);
            if (failOnCorruptedLogFiles) {
                throw e;
            }
        }

        if (channel.getLogVersion() != logVersion) {
            throw bridgeChannelException(channel, logVersion);
        }

        long totalSize = channel.position();

        if (failOnLastBrokenEntry
                && (logEntryReader.hasBrokenLastEntry() || (maxOffset != Long.MAX_VALUE && totalSize != maxOffset))) {
            // Checking the offset we got to as well since the logEntry reader does not report ReadPastEnd
            // exceptions as broken last entry.
            throw new IllegalStateException(
                    totalSize != maxOffset
                            ? "Log file ended (at %s) before requested maxOffset %s".formatted(totalSize, maxOffset)
                            : "Log file ends with a last broken entry which is not okay when recovering to a specific position."
                                    + " All broken parts should already have been truncated at this point.");
        }

        return new ReversedSingleFileCommandBatchCursor(channel, logEntryReader, offsets, offsetCursor, totalSize);
    }

    private ReversedSingleFileCommandBatchCursor(
            ReadAheadLogChannel channel,
            LogEntryReader logEntryReader,
            long[] offsets,
            int offsetCursor,
            long totalSize)
            throws IOException {
        this.channel = channel;
        this.offsets = offsets;
        this.offsetsLength = offsetCursor;
        this.chunkStartOffsetIndex = offsetCursor;
        this.totalSize = totalSize;

        // There's an assumption here: that the underlying channel can move in between calls and that the
        // transaction cursor will just happily read from the new position.
        this.commandBatchCursor = new CommittedCommandBatchCursor(channel, logEntryReader);
    }

    @Override
    public boolean next() throws IOException {
        if (!exhausted()) {
            if (currentChunkExhausted()) {
                readNextChunk();
            }
            ReservedBatch reservedBatch = chunkBatches.pop();
            currentCommandBatch = reservedBatch.commitedBatch();
            currentBatchStartPosition = reservedBatch.batchStartPosition();
            return true;
        }
        return false;
    }

    private void readNextChunk() throws IOException {
        assert chunkStartOffsetIndex > 0;

        // Start at lowOffsetIndex - 1 and count backwards until almost reaching the chunk size
        long highOffset = chunkStartOffsetIndex == offsetsLength ? totalSize : offsets[chunkStartOffsetIndex];
        int newLowOffsetIndex = chunkStartOffsetIndex;
        while (newLowOffsetIndex > 0) {
            long deltaOffset = highOffset - offsets[--newLowOffsetIndex];
            if (deltaOffset > CHUNK_SIZE) {
                // We've now read more than the read-ahead size, let's call this the end of this chunk
                break;
            }
        }
        assert chunkStartOffsetIndex - newLowOffsetIndex > 0;

        // We've established the chunk boundaries. Initialize all offsets and read the transactions in this
        // chunk into actual transaction objects
        int chunkLength = chunkStartOffsetIndex - newLowOffsetIndex;
        chunkStartOffsetIndex = newLowOffsetIndex;
        channel.position(offsets[chunkStartOffsetIndex]);
        assert chunkBatches.isEmpty();
        for (int i = 0; i < chunkLength; i++) {
            boolean success = commandBatchCursor.next();
            assert success;
            var batchStartPosition = new LogPosition(channel.getLogVersion(), offsets[chunkStartOffsetIndex + i]);
            chunkBatches.push(new ReservedBatch(commandBatchCursor.get(), batchStartPosition));
        }
    }

    private boolean currentChunkExhausted() {
        return chunkBatches.isEmpty();
    }

    private boolean exhausted() {
        return chunkStartOffsetIndex == 0 && currentChunkExhausted();
    }

    @Override
    public void close() throws IOException {
        commandBatchCursor.close(); // closes the channel too
    }

    @Override
    public CommittedCommandBatchRepresentation get() {
        return currentCommandBatch;
    }

    @Override
    public LogPosition position() {
        return currentBatchStartPosition;
    }

    private static IllegalArgumentException bridgeChannelException(ReadAheadLogChannel channel, long logVersion) {
        return new IllegalArgumentException(
                "The channel which was passed in bridged multiple log versions, it started at version " + logVersion
                        + ", but continued through to version " + channel.getLogVersion()
                        + ". This isn't supported");
    }
}
