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

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.wal.entry.LogEnvelopeHeader.EnvelopeType.START_OFFSET;
import static org.neo4j.wal.entry.LogEnvelopeHeader.HEADER_SIZE;
import static org.neo4j.wal.entry.LogEnvelopeHeader.MAX_ZERO_PADDING_SIZE;
import static org.neo4j.wal.entry.LogEnvelopeHeader.OFFSET_CHECKSUM;
import static org.neo4j.wal.entry.LogEnvelopeHeader.OFFSET_ENVELOPE_TYPE;
import static org.neo4j.wal.entry.LogEnvelopeHeader.OFFSET_KERNEL_VERSION;
import static org.neo4j.wal.entry.LogEnvelopeHeader.OFFSET_PAYLOAD_LENGTH;
import static org.neo4j.wal.entry.LogEnvelopeHeader.OFFSET_PREVIOUS_CHECKSUM;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.io.fs.ChecksumMismatchException;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;

/**
 * Pins that {@link EnvelopeWriteChannel#appendRaw} rejects invalid input without corrupting channel state: an
 * envelope that fails its CRC, breaks the tracked checksum chain or disagrees with the caller's index/term is
 * rejected without advancing the chain, and a torn entry — a chunk ending mid-envelope, or a last envelope that
 * leaves the entry open — is rejected at the append call itself. Degenerate input is pinned too: an entry shipping
 * no envelopes — an empty chunk array or a padding-only run — is rejected, and forged or misaligned envelopes —
 * out-of-bounds payload lengths, zero runs away from a segment tail, non-envelope envelope types, envelopes off
 * this log's segment grid — are rejected outright, never hung on or silently written.
 */
@TestDirectoryExtension
class EnvelopeWriteChannelRawRejectionTest extends EnvelopeWriteChannelTestSupport {

    @Test
    void rejectsDoctoredPreviousChecksumEvenWithValidCrc() throws IOException {
        int segmentSize = 128;
        ByteBuffer envelope = zeroBased(rawEntry(segmentSize, bytes(random, 20), T1));
        int doctored = 0xBADC0DE;
        envelope.putInt(OFFSET_PREVIOUS_CHECKSUM, doctored);
        envelope.putInt(OFFSET_CHECKSUM, envelopeCrc(envelope));

        try (var channel = writeChannel(storeChannel(0), segmentSize, buffer(segmentSize * 8))) {
            // the recomputed CRC is self-consistent, so only the chain check can catch the forgery
            assertThatThrownBy(() -> channel.appendRaw(chunks(envelope), FIRST_INDEX, T1))
                    .isInstanceOf(ChecksumMismatchException.class)
                    .hasMessageContaining("checksum chain")
                    .hasMessageContaining("file position " + segmentSize)
                    .hasMessageContaining("index " + FIRST_INDEX)
                    .hasMessageContaining("term " + T1)
                    .hasMessageContaining(String.valueOf(doctored))
                    .hasMessageContaining(String.valueOf(BASE_TX_CHECKSUM));
            assertChecksum(channel.currentChecksum(), BASE_TX_CHECKSUM);
        }
    }

    @Test
    void rejectsCorruptPayloadByte() throws IOException {
        int segmentSize = 128;
        ByteBuffer envelope = zeroBased(rawEntry(segmentSize, bytes(random, 20), T1));
        int storedChecksum = envelope.getInt(OFFSET_CHECKSUM);
        int corruptOffset = HEADER_SIZE + 3;
        envelope.put(corruptOffset, (byte) (envelope.get(corruptOffset) ^ 0x5A));
        int checksumOfCorrupted = envelopeCrc(envelope);

        try (var channel = writeChannel(storeChannel(0), segmentSize, buffer(segmentSize * 8))) {
            assertThatThrownBy(() -> channel.appendRaw(chunks(envelope), FIRST_INDEX, T1))
                    .isInstanceOf(ChecksumMismatchException.class)
                    .hasMessageContaining("stored checksum")
                    .hasMessageContaining(String.valueOf(storedChecksum))
                    .hasMessageContaining(String.valueOf(checksumOfCorrupted))
                    // envelope identity, so a corrupt shipment is correlatable with the leader log
                    .hasMessageContaining("file position " + segmentSize)
                    .hasMessageContaining("index " + FIRST_INDEX)
                    .hasMessageContaining("term " + T1);
            assertChecksum(channel.currentChecksum(), BASE_TX_CHECKSUM);
        }
    }

    @Test
    void rejectsCorruptHeaderByte() throws IOException {
        int segmentSize = 128;
        ByteBuffer envelope = zeroBased(rawEntry(segmentSize, bytes(random, 20), T1));
        int storedChecksum = envelope.getInt(OFFSET_CHECKSUM);
        envelope.put(OFFSET_KERNEL_VERSION, (byte) (envelope.get(OFFSET_KERNEL_VERSION) ^ 0x1));
        int checksumOfCorrupted = envelopeCrc(envelope);

        try (var channel = writeChannel(storeChannel(0), segmentSize, buffer(segmentSize * 8))) {
            assertThatThrownBy(() -> channel.appendRaw(chunks(envelope), FIRST_INDEX, T1))
                    .isInstanceOf(ChecksumMismatchException.class)
                    .hasMessageContaining("stored checksum")
                    .hasMessageContaining(String.valueOf(storedChecksum))
                    .hasMessageContaining(String.valueOf(checksumOfCorrupted))
                    .hasMessageContaining("file position " + segmentSize)
                    .hasMessageContaining("index " + FIRST_INDEX)
                    .hasMessageContaining("term " + T1);
            assertChecksum(channel.currentChecksum(), BASE_TX_CHECKSUM);
        }
    }

    @Test
    void rejectsIndexSkewBetweenEnvelopeAndCall() throws IOException {
        int segmentSize = 128;
        ByteBuffer envelope = zeroBased(rawEntry(segmentSize, bytes(random, 20), T1));

        try (var channel = writeChannel(storeChannel(0), segmentSize, buffer(segmentSize * 8))) {
            assertThatThrownBy(() -> channel.appendRaw(chunks(envelope), FIRST_INDEX + 7, T1))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("file position " + segmentSize)
                    .hasMessageContaining("has index " + FIRST_INDEX)
                    .hasMessageContaining("called with index " + (FIRST_INDEX + 7));
            assertChecksum(channel.currentChecksum(), BASE_TX_CHECKSUM);
        }
    }

    @Test
    void rejectsTermSkewBetweenEnvelopeAndCall() throws IOException {
        int segmentSize = 128;
        ByteBuffer envelope = zeroBased(rawEntry(segmentSize, bytes(random, 20), T1));

        try (var channel = writeChannel(storeChannel(0), segmentSize, buffer(segmentSize * 8))) {
            assertThatThrownBy(() -> channel.appendRaw(chunks(envelope), FIRST_INDEX, T1 + 37))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("file position " + segmentSize)
                    .hasMessageContaining("index " + FIRST_INDEX)
                    .hasMessageContaining("has term " + T1)
                    .hasMessageContaining("called with term " + (T1 + 37));
            assertChecksum(channel.currentChecksum(), BASE_TX_CHECKSUM);
        }
    }

    @Test
    void rejectsSecondEntryDeliveredUnderOneIndexAndTerm() throws IOException {
        int segmentSize = 128;
        // both entries carry T1 so the index check is the only thing that can reject the second one
        var entries = rawEntries(segmentSize, bytes(random, 20), T1, bytes(random, 24), T1);
        ByteBuffer bothEntries = entries[0].duplicate().limit(entries[1].limit());
        int firstEntryChecksum = entries[0].getInt(entries[0].position() + OFFSET_CHECKSUM);

        try (var channel = writeChannel(storeChannel(0), segmentSize, buffer(segmentSize * 8))) {
            assertThatThrownBy(() -> channel.appendRaw(chunks(bothEntries), FIRST_INDEX, T1))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("file position " + (segmentSize + HEADER_SIZE + 20))
                    .hasMessageContaining("has index " + (FIRST_INDEX + 1))
                    .hasMessageContaining("called with index " + FIRST_INDEX);
            assertChecksum(channel.currentChecksum(), firstEntryChecksum);
        }
    }

    @Test
    void rejectsFirstEnvelopeNotChainingFromSeedChecksum() throws IOException {
        int segmentSize = 128;
        ByteBuffer envelope = zeroBased(rawEntry(segmentSize, bytes(random, 20), T1));
        int wrongSeed = BASE_TX_CHECKSUM + 1;

        try (var channel = writeChannel(storeChannel(0), segmentSize, wrongSeed, buffer(segmentSize * 8))) {
            assertThatThrownBy(() -> channel.appendRaw(chunks(envelope), FIRST_INDEX, T1))
                    .isInstanceOf(ChecksumMismatchException.class)
                    .hasMessageContaining("checksum chain")
                    .hasMessageContaining("file position " + segmentSize)
                    .hasMessageContaining("index " + FIRST_INDEX)
                    .hasMessageContaining("term " + T1)
                    .hasMessageContaining(String.valueOf(BASE_TX_CHECKSUM))
                    .hasMessageContaining(String.valueOf(wrongSeed));
            assertChecksum(channel.currentChecksum(), wrongSeed);
        }
    }

    @Test
    void tornEntryArrayIsRejectedAtTheAppendCall() throws IOException {
        int segmentSize = 128;
        // header + 96 bytes fill the BEGIN envelope exactly to the first boundary; the END envelope is never delivered
        byte[] payload = bytes(random, segmentSize - HEADER_SIZE + 20);
        ByteBuffer entry = zeroBased(rawEntry(segmentSize, payload, T1));
        int beginEnvelopeChecksum = entry.getInt(OFFSET_CHECKSUM);
        ByteBuffer beginEnvelopeOnly = entry.duplicate().limit(segmentSize);

        try (var channel = writeChannel(storeChannel(0), segmentSize, buffer(segmentSize * 8))) {
            // every envelope is whole, but the last one leaves the entry open
            assertThatThrownBy(() -> channel.appendRaw(chunks(beginEnvelopeOnly), FIRST_INDEX, T1))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("torn")
                    .hasMessageContaining("open")
                    .hasMessageContaining("index " + FIRST_INDEX);
            // the complete BEGIN envelope was mirrored before the structural rejection (rotation seeding relies on it)
            assertChecksum(channel.currentChecksum(), beginEnvelopeChecksum);
            // the tear is permanent: local writes stay rejected on the broken channel
            assertThatThrownBy(channel::beginChecksumForWriting)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("still open");
        }

        ByteBuffer envelope = zeroBased(rawEntry(segmentSize, bytes(random, 20), T1));
        int cut = HEADER_SIZE + 8;
        try (var channel = writeChannel(storeChannel(1), segmentSize, buffer(segmentSize * 8))) {
            // the last chunk ends mid-envelope: rejected without advancing the chain
            assertThatThrownBy(
                            () -> channel.appendRaw(chunks(envelope.duplicate().limit(cut)), FIRST_INDEX, T1))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("torn")
                    .hasMessageContaining(cut + " bytes");
            assertChecksum(channel.currentChecksum(), BASE_TX_CHECKSUM);
        }

        try (var channel = writeChannel(storeChannel(2), segmentSize, buffer(segmentSize * 8))) {
            // a tear between chunks is rejected at the offending chunk, even though the next chunk would
            // complete the envelope: envelopes must be whole per chunk, not just per call
            assertThatThrownBy(() -> channel.appendRaw(
                            chunks(
                                    envelope.duplicate().limit(cut),
                                    envelope.duplicate().position(cut)),
                            FIRST_INDEX,
                            T1))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("torn")
                    .hasMessageContaining(cut + " bytes of an incomplete envelope");
            assertChecksum(channel.currentChecksum(), BASE_TX_CHECKSUM);
        }
    }

    @Test
    void tornCutShorterThanTypeFieldIsRejected() throws IOException {
        int segmentSize = 128;
        // 3 bytes cannot even say whether an envelope or a padding run starts here: the parser must leave them
        // unconsumed for the torn rejection, not misread the absent type byte
        ByteBuffer envelope = zeroBased(rawEntry(segmentSize, bytes(random, 20), T1));
        int cut = OFFSET_ENVELOPE_TYPE - 1;

        try (var channel = writeChannel(storeChannel(0), segmentSize, buffer(segmentSize * 8))) {
            assertThatThrownBy(
                            () -> channel.appendRaw(chunks(envelope.duplicate().limit(cut)), FIRST_INDEX, T1))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("torn")
                    .hasMessageContaining(cut + " bytes of an incomplete envelope");
            assertChecksum(channel.currentChecksum(), BASE_TX_CHECKSUM);
        }
    }

    @Test
    void tornCutBeforePayloadLengthFieldIsRejected() throws IOException {
        int segmentSize = 128;
        // the type byte is just readable but the payload length is entirely absent: the parser must wait for
        // the field, not misread zeros as the length (a cut inside the field would misread the length's low
        // byte and coincidentally still land on the torn rejection)
        ByteBuffer envelope = zeroBased(rawEntry(segmentSize, bytes(random, 20), T1));
        int cut = OFFSET_PAYLOAD_LENGTH;

        try (var channel = writeChannel(storeChannel(0), segmentSize, buffer(segmentSize * 8))) {
            assertThatThrownBy(
                            () -> channel.appendRaw(chunks(envelope.duplicate().limit(cut)), FIRST_INDEX, T1))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("torn")
                    .hasMessageContaining(cut + " bytes of an incomplete envelope");
            assertChecksum(channel.currentChecksum(), BASE_TX_CHECKSUM);
        }
    }

    @Test
    void rejectsEntryShippingNoEnvelopes() throws IOException {
        int segmentSize = 128;
        // entry 1 leaves a 20-byte tail: short enough that the pre-write padding logic would pad it if it ran
        var source = writeSourceEntries(
                segmentSize, 8, new byte[][] {bytes(random, 77), bytes(random, 20)}, new long[] {T1, T2});
        var entry1 = source.entries().get(0);
        var entry2 = source.entries().get(1);
        assertThat(2 * segmentSize - entry1.unpaddedEnd()).isEqualTo(20);

        try (var channel = writeChannel(storeChannel(0), segmentSize, buffer(segmentSize * 8))) {
            channel.appendRaw(chunks(entryChunk(source, 0)), entry1.index(), entry1.term());
            long positionBefore = channel.position();
            long appendedBefore = channel.getAppendedBytes();

            // the caller advances its log index per appended entry: silently accepting an envelope-less entry would
            // surface one entry later as a chain mismatch blaming the wrong entry
            assertThatThrownBy(() -> channel.appendRaw(chunks(), entry1.index() + 1, T2))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("no envelopes")
                    .hasMessageContaining("index " + (entry1.index() + 1))
                    .hasMessageContaining("term " + T2);
            assertThatThrownBy(() -> channel.appendRaw(chunks(ByteBuffer.allocate(0)), entry1.index() + 1, T2))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("no envelopes");

            // nothing was appended: the rejection leaves no trace and the channel stays usable
            assertThat(channel.position()).isEqualTo(positionBefore);
            assertThat(channel.getAppendedBytes()).isEqualTo(appendedBefore);
            assertChecksum(channel.currentChecksum(), entry1.checksum());
            assertThat(channel.currentIndex()).isEqualTo(entry1.index());
            assertThat(channel.currentTerm()).isEqualTo(entry1.term());

            channel.appendRaw(chunks(entryChunk(source, 1)), entry2.index(), entry2.term());
            assertChecksum(channel.currentChecksum(), entry2.checksum());
        }

        assertThat(fileBytes(0)).isEqualTo(fileBytes(SOURCE_LOG_VERSION));
    }

    @Test
    void rejectsPaddingOnlyEntry() throws IOException {
        int segmentSize = 128;
        var source = writeDeadZoneSource(segmentSize);
        var entry1 = source.entries().get(0);
        // a dead-zone run shipped alone under its own append: like an all-empty entry, the caller would advance
        // its log index with no envelope on disk
        ByteBuffer paddingOnly = zeroBased(source.data()
                .duplicate()
                .position(entry1.unpaddedEnd())
                .limit(2 * segmentSize)
                .order(LITTLE_ENDIAN));
        assertThat(paddingOnly.remaining()).isEqualTo(HEADER_SIZE + 2);

        try (var channel = writeChannel(storeChannel(0), segmentSize, buffer(segmentSize * 8))) {
            channel.appendRaw(chunks(entryChunk(source, 0)), entry1.index(), entry1.term());

            assertThatThrownBy(() -> channel.appendRaw(chunks(paddingOnly), entry1.index() + 1, T2))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("no envelopes")
                    .hasMessageContaining("index " + (entry1.index() + 1))
                    .hasMessageContaining("term " + T2);

            assertChecksum(channel.currentChecksum(), entry1.checksum());
            // fail-stop: the rejected padding bytes sit above the last completed envelope, so a local write
            // must not start on top of them and flush them into the log
            assertThatThrownBy(channel::beginChecksumForWriting)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("still open")
                    .hasMessageContaining((HEADER_SIZE + 2) + " bytes");
        }
    }

    /**
     * The three edges of the {@code (0, capacity]} payload-length bound, where capacity is the payload space of
     * the 128-byte segment used below. The bound must reject on the length field alone, not wait for an envelope
     * that cannot fit. Every other field is chain-valid and the CRC is recomputed over the forged extent
     * (clamped to the header-only chunk), so only the bound can reject.
     */
    @ParameterizedTest(name = "payloadLength={0}")
    @ValueSource(ints = {0, -5, 128 - HEADER_SIZE + 1})
    void rejectsOutOfBoundsPayloadLength(int payloadLength) throws IOException {
        int segmentSize = 128;
        ByteBuffer envelope =
                zeroBased(rawEntry(segmentSize, bytes(random, 20), T1)).limit(HEADER_SIZE);
        envelope.putInt(OFFSET_PAYLOAD_LENGTH, payloadLength);
        int crcExtent = Math.min(HEADER_SIZE + payloadLength, envelope.limit());
        envelope.putInt(OFFSET_CHECKSUM, envelopeCrc(envelope.duplicate().limit(crcExtent)));

        try (var channel = writeChannel(storeChannel(0), segmentSize, buffer(segmentSize * 8))) {
            assertThatThrownBy(() -> channel.appendRaw(chunks(envelope), FIRST_INDEX, T1))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("payload length " + payloadLength)
                    .hasMessageContaining("index " + FIRST_INDEX)
                    .hasMessageContaining(String.valueOf(segmentSize - HEADER_SIZE));
            assertChecksum(channel.currentChecksum(), BASE_TX_CHECKSUM);
        }
    }

    /**
     * The same bound at the pre-write peek, which runs before any byte is copied: mid-segment the next envelope's
     * header decides whether the tail is due padding, so a forged length must be rejected on the field alone —
     * before the peek pads, and possibly rotates, on garbage.
     */
    @ParameterizedTest(name = "payloadLength={0}")
    @ValueSource(ints = {0, -5, 128 - HEADER_SIZE + 1, 5000})
    void rejectsOutOfBoundsPayloadLengthAtPeek(int payloadLength) throws IOException {
        int segmentSize = 128;
        // envelope A leaves a tail short enough to be padding, so the peek consults envelope B's header
        ByteBuffer envelopeA = zeroBased(rawEntry(segmentSize, bytes(random, 77), T1));
        int checksumA = envelopeA.getInt(OFFSET_CHECKSUM);
        ByteBuffer envelopeB = zeroBased(rawEntry(2 * segmentSize, bytes(random, 60), T2, checksumA, FIRST_INDEX, T1));
        envelopeB.putInt(OFFSET_PAYLOAD_LENGTH, payloadLength);

        try (var channel = writeChannel(storeChannel(0), segmentSize, buffer(segmentSize * 8))) {
            channel.appendRaw(chunks(envelopeA), FIRST_INDEX, T1);
            long positionAfterA = channel.position();

            assertThatThrownBy(() -> channel.appendRaw(chunks(envelopeB), FIRST_INDEX + 1, T2))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("peeked payload length " + payloadLength)
                    .hasMessageContaining("index " + (FIRST_INDEX + 1))
                    .hasMessageContaining(String.valueOf(segmentSize - HEADER_SIZE));
            assertThat(channel.position())
                    .as("rejected on the length field, before the peek padded the tail")
                    .isEqualTo(positionAfterA);
            assertChecksum(channel.currentChecksum(), checksumA);
        }
    }

    @Test
    void rejectsZeroRunFarFromSegmentTail() throws IOException {
        int segmentSize = 128;
        // 64 zeros at the segment start: further from the boundary than any legal padding run can be
        ByteBuffer zeros = ByteBuffer.allocate(64);

        try (var channel = writeChannel(storeChannel(0), segmentSize, buffer(segmentSize * 8))) {
            assertThatThrownBy(() -> channel.appendRaw(chunks(zeros), FIRST_INDEX, T1))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("padding")
                    .hasMessageContaining("index " + FIRST_INDEX)
                    .hasMessageContaining(String.valueOf(MAX_ZERO_PADDING_SIZE));
            assertChecksum(channel.currentChecksum(), BASE_TX_CHECKSUM);
        }
    }

    @Test
    void rejectsNonZeroByteInShortTailPaddingRun() throws IOException {
        int segmentSize = 128;
        // payload 77 leaves a 20-byte tail (within HEADER_SIZE of the boundary): the parser can only take it
        // for a padding run, so any non-zero byte in it is corrupt shipping
        ByteBuffer envelope = zeroBased(rawEntry(segmentSize, bytes(random, 77), T1));
        int envelopeChecksum = envelope.getInt(OFFSET_CHECKSUM);
        int dirtyOffset = envelope.remaining() + 7;
        ByteBuffer chunk = ByteBuffer.allocate(segmentSize).order(LITTLE_ENDIAN);
        chunk.put(envelope.duplicate());
        chunk.put(dirtyOffset, (byte) 0x42);
        chunk.position(0);

        try (var channel = writeChannel(storeChannel(0), segmentSize, buffer(segmentSize * 8))) {
            assertThatThrownBy(() -> channel.appendRaw(chunks(chunk), FIRST_INDEX, T1))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("padding")
                    .hasMessageContaining("byte " + 0x42)
                    .hasMessageContaining("file position " + (segmentSize + dirtyOffset));
            assertChecksum(channel.currentChecksum(), envelopeChecksum);
        }
    }

    @Test
    void rejectsCorruptByteInsideDeadZonePaddingRun() throws IOException {
        int segmentSize = 128;
        var source = writeDeadZoneSource(segmentSize);
        var entry1 = source.entries().get(0);
        // corrupt a byte past the run's type byte so the run still classifies as ZERO padding and only the
        // all-zero verification can reject it
        ByteBuffer paddingRun = zeroBased(source.data()
                .duplicate()
                .position(entry1.unpaddedEnd())
                .limit(2 * segmentSize)
                .order(LITTLE_ENDIAN));
        int corruptOffset = OFFSET_ENVELOPE_TYPE + 2;
        paddingRun.put(corruptOffset, (byte) 0x17);

        try (var channel = writeChannel(storeChannel(0), segmentSize, buffer(segmentSize * 8))) {
            channel.appendRaw(chunks(entryChunk(source, 0)), entry1.index(), entry1.term());

            assertThatThrownBy(() -> channel.appendRaw(chunks(paddingRun), entry1.index(), entry1.term()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("padding")
                    .hasMessageContaining("byte " + 0x17)
                    .hasMessageContaining("file position " + (entry1.unpaddedEnd() + corruptOffset));
            assertChecksum(channel.currentChecksum(), entry1.checksum());
            assertThat(channel.currentIndex()).isEqualTo(entry1.index());
            assertThat(channel.currentTerm()).isEqualTo(entry1.term());
        }
    }

    @Test
    void rejectsStartOffsetEnvelopeInRawStream() throws IOException {
        int segmentSize = 128;
        // the production decoder never ships START_OFFSET envelopes, so one in a raw stream is forged input;
        // every other field is chain-valid and the CRC recomputed, leaving the envelope-type check as sole rejector
        ByteBuffer envelope = zeroBased(rawEntry(segmentSize, bytes(random, 20), T1));
        envelope.put(OFFSET_ENVELOPE_TYPE, START_OFFSET.typeValue);
        envelope.putInt(OFFSET_CHECKSUM, envelopeCrc(envelope));

        try (var channel = writeChannel(storeChannel(0), segmentSize, buffer(segmentSize * 8))) {
            assertThatThrownBy(() -> channel.appendRaw(chunks(envelope), FIRST_INDEX, T1))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("envelope type " + START_OFFSET.typeValue)
                    .hasMessageContaining("index " + FIRST_INDEX);
            assertChecksum(channel.currentChecksum(), BASE_TX_CHECKSUM);
        }
    }

    @Test
    void rejectsEnvelopeStraddlingSegmentBoundary() throws IOException {
        int segmentSize = 128;
        // envelope A is legal; envelope B (91 bytes, built on a 256-byte segment grid) starts at 200 and would straddle
        // the boundary at 256. A first-envelope overrun is rejected by the pre-write peek; riding mid-chunk behind
        // envelope A, only the parser's per-position payload bound can catch it — with or without the rest of the
        // envelope's bytes delivered — instead of waiting for an envelope that cannot fit
        ByteBuffer envelopeA = zeroBased(rawEntry(segmentSize, bytes(random, 41), T1));
        int checksumA = envelopeA.getInt(OFFSET_CHECKSUM);
        ByteBuffer envelopeB = zeroBased(rawEntry(2 * segmentSize, bytes(random, 60), T2, checksumA, FIRST_INDEX, T1));
        int spaceLeftInSegment = segmentSize - envelopeA.remaining() - HEADER_SIZE;
        ByteBuffer chunk = ByteBuffer.allocate(envelopeA.remaining() + 59).order(LITTLE_ENDIAN);
        chunk.put(envelopeA.duplicate()).put(envelopeB.duplicate().limit(59)).flip();

        try (var channel = writeChannel(storeChannel(0), segmentSize, buffer(segmentSize * 8))) {
            assertThatThrownBy(() -> channel.appendRaw(chunks(chunk), FIRST_INDEX, T1))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("payload length 60")
                    .hasMessageContaining("index " + FIRST_INDEX)
                    .hasMessageContaining(String.valueOf(spaceLeftInSegment));
            assertChecksum(channel.currentChecksum(), checksumA);
        }
    }

    @Test
    void rejectsEntryStartEnvelopeMisalignedToSegmentGrid() throws IOException {
        int segmentSize = 128;
        // envelope A leaves a 77-byte tail: too wide to be padding (> MAX_ZERO_PADDING_SIZE) yet too narrow for
        // envelope B (91 bytes, built on a 256-byte grid). B is chain-valid, so only the pre-write peek's
        // misalignment check can reject it instead of silently padding the 77 bytes
        ByteBuffer envelopeA = zeroBased(rawEntry(segmentSize, bytes(random, 20), T1));
        int checksumA = envelopeA.getInt(OFFSET_CHECKSUM);
        int tail = segmentSize - envelopeA.limit();
        ByteBuffer envelopeB = zeroBased(rawEntry(2 * segmentSize, bytes(random, 60), T2, checksumA, FIRST_INDEX, T1));

        try (var channel = writeChannel(storeChannel(0), segmentSize, buffer(segmentSize * 8))) {
            channel.appendRaw(chunks(envelopeA), FIRST_INDEX, T1);

            assertThatThrownBy(() -> channel.appendRaw(chunks(envelopeB), FIRST_INDEX + 1, T2))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(tail + "-byte tail")
                    .hasMessageContaining(envelopeB.limit() + "-byte envelope")
                    .hasMessageContaining("misaligned");
            assertChecksum(channel.currentChecksum(), checksumA);
        }
    }
}
