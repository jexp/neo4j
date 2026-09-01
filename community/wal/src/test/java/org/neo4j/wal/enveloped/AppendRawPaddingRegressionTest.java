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
package org.neo4j.wal.enveloped;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.io.fs.ReadableChannel.BASE_TERM;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.wal.entry.LogEnvelopeHeader.HEADER_SIZE;
import static org.neo4j.wal.enveloped.LatestVersions.LATEST_KERNEL_VERSION;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;

/**
 * Pins the two padding behaviours {@link EnvelopeWriteChannel#appendRaw} must keep for binary log shipping:
 * <ol>
 *     <li>Range-boundary padding re-creation: the log shipper's range ends at goToEndOfEntry (before trailing
 *     padding) and the next range starts at the next entry's first envelope (after the padding), so segment-tail
 *     padding between two shipped ranges never travels on the wire — the receiver must re-create it locally.</li>
 *     <li>Dead-zone padding acceptance: the pojo writer emits 32-39 bytes of zero padding when a fixed-size
 *     primitive does not fit as the first payload write (production enveloped raft entries start with
 *     ReplicationMetadataMarshal's putInt). The decoder folds that padding into the shipped chunk, so a raw append
 *     must accept a padding run longer than HEADER_SIZE.</li>
 * </ol>
 */
@TestDirectoryExtension
class AppendRawPaddingRegressionTest extends EnvelopeWriteChannelTestSupport {
    private static final int RANGE_TEST_SEGMENT_SIZE = 256;

    /**
     * Binary log shipping never delivers segment-tail padding (a range ends at goToEndOfEntry, before the padding;
     * the next range starts at the next entry's first envelope, after it). Replaying the source one envelope envelope per
     * chunk - [envelope start .. unpadded end), a split entry's BEGIN/END arriving as separate chunks of one call -
     * must reproduce the source file byte for byte, with appendRaw re-creating the padding locally.
     */
    @Test
    void appendRawPerEnvelopeWithoutTrailingPaddingReproducesSourceFile() throws IOException {
        int segmentSize = RANGE_TEST_SEGMENT_SIZE;
        try (var storeChannel = srcStoreChannel()) {
            writeEnvelopeData(storeChannel, segmentSize);
        }
        Map<Long, EnvelopeInfo> envelopes;
        try (var storeChannel = srcStoreChannel()) {
            envelopes = readEnvelopeData(storeChannel, segmentSize);
        }
        // Sanity: the source must contain a zero-padded segment boundary for this test to prove anything.
        // The last envelope is excluded: its endPosition is the file size, not the next envelope's start.
        boolean hasPaddedBoundary = false;
        for (long i = 0; i < envelopes.size() - 1; i++) {
            var env = envelopes.get(i);
            if (env.endPosition() % segmentSize == 0 && env.unpaddedEndPosition() % segmentSize != 0) {
                hasPaddedBoundary = true;
                break;
            }
        }
        assertThat(hasPaddedBoundary)
                .as("source contains a zero-padded segment tail")
                .isTrue();

        try (var srcStoreChannel = srcStoreChannel();
                var destStoreChannel = destStoreChannel()) {
            copySegmentZero(srcStoreChannel, destStoreChannel, segmentSize);
            try (var writer = createWriter(destStoreChannel, segmentSize, -1L)) {
                for (long i = 0; i < envelopes.size(); ) {
                    long end = entryEnd(envelopes, i);
                    var chunks = new ByteBuffer[(int) (end - i)];
                    for (long f = i; f < end; f++) {
                        var env = envelopes.get(f);
                        chunks[(int) (f - i)] = chunk(srcStoreChannel, env.beginPosition(), env.unpaddedEndPosition());
                    }
                    var last = envelopes.get(end - 1);
                    writer.appendRaw(chunks, last.appendIndex(), last.term());
                    i = end;
                }
                writer.prepareForFlush().flush();
            }
        }
        assertThat(fileBytes(destLogPath())).isEqualTo(fileBytes(srcLogPath()));
    }

    /**
     * An entry ending 33 bytes before a segment boundary followed by an entry whose first payload write is putInt
     * (as production entries do: ReplicationMetadataMarshal writes the metadata block size first) makes the writer
     * discard the empty envelope and zero-pad 33 bytes - more than HEADER_SIZE. The decoder ships that padding folded
     * into the next entry's chunk, so a replay of the file bytes in those envelope-aligned chunks must accept it.
     */
    @Test
    void appendRawAcceptsDeadZonePaddingLargerThanHeaderSize() throws IOException {
        try (var storeChannel = srcStoreChannel()) {
            writeV10Header(storeChannel, SEGMENT_SIZE, -1L, BASE_TERM, BASE_TX_CHECKSUM);
            try (var writer = createWriter(storeChannel, SEGMENT_SIZE, -1L)) {
                // Entry 1: FULL envelope ending 33 bytes short of the segment boundary.
                int payload = SEGMENT_SIZE - 33 - HEADER_SIZE;
                writer.beginChecksumForWriting();
                writer.putContentType((byte) 1);
                writer.putVersion(LATEST_KERNEL_VERSION.version());
                writer.putTerm(1L);
                writer.put(new byte[payload], 0, payload);
                writer.putChecksum();
                // Entry 2: first payload write is an int, which does not fit in the 2 bytes left after the header
                // reservation -> empty-envelope discard -> 33 bytes of zero padding (the dead zone: > HEADER_SIZE).
                writer.beginChecksumForWriting();
                writer.putContentType((byte) 2);
                writer.putVersion(LATEST_KERNEL_VERSION.version());
                writer.putTerm(2L);
                writer.putInt(42);
                writer.put(new byte[16], 0, 16);
                writer.putChecksum();
                writer.prepareForFlush().flush();
            }
        }
        // Sanity: the dead-zone padding is really on disk.
        var srcBytes = fileBytes(srcLogPath());
        int padStart = SEGMENT_SIZE + (SEGMENT_SIZE - 33);
        for (int i = padStart; i < 2 * SEGMENT_SIZE; i++) {
            assertThat(srcBytes[i]).as("padding byte at %d", i).isZero();
        }

        try (var destStoreChannel = destStoreChannel()) {
            destStoreChannel.write(ByteBuffer.wrap(srcBytes, 0, SEGMENT_SIZE));
            try (var writer = createWriter(destStoreChannel, SEGMENT_SIZE, -1L)) {
                // chunks as the decoder cuts them: entry 1's envelope alone, then the folded dead-zone padding
                // leading entry 2's envelope
                writer.appendRaw(chunks(ByteBuffer.wrap(srcBytes, SEGMENT_SIZE, padStart - SEGMENT_SIZE)), 0, 1);
                writer.appendRaw(chunks(ByteBuffer.wrap(srcBytes, padStart, srcBytes.length - padStart)), 1, 2);
                writer.prepareForFlush().flush();
            }
        }
        assertThat(fileBytes(destLogPath())).isEqualTo(fileBytes(srcLogPath()));
    }
}
