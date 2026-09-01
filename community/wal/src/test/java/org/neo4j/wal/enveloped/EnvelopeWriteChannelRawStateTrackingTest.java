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
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;
import static org.neo4j.wal.entry.LogEnvelopeHeader.EnvelopeType.FULL;
import static org.neo4j.wal.entry.LogEnvelopeHeader.HEADER_SIZE;
import static org.neo4j.wal.entry.LogEnvelopeHeader.OFFSET_CHECKSUM;
import static org.neo4j.wal.entry.LogEnvelopeHeader.OFFSET_PREVIOUS_CHECKSUM;
import static org.neo4j.wal.rotation.LogRotation.NO_ROTATION;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.neo4j.io.fs.ChecksumMismatchException;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.wal.LogTracers;

/**
 * Pins that {@link EnvelopeWriteChannel#appendRaw} mirrors channel state from every raw envelope of the appended
 * entry: the checksum chain, index and term follow the shipped envelopes (observable via {@code currentChecksum()}/
 * {@code currentIndex()}/{@code currentTerm()}), and padding runs advance no state. The chain is also what pins
 * each envelope being parsed exactly once: every parse validates its previous-checksum link before advancing it, so
 * a skipped, repeated or misparsed envelope leaves the chain somewhere other than the shipped entry's own checksum.
 * Bytes written by the non-raw paths (pojo envelopes, START_OFFSET, directPutAll) are never parsed by the raw walk,
 * across flushes, buffer-window wraps, mid-segment reopens and truncation reseeds. Local (pojo) writes directly
 * after raw appends chain seamlessly with no recalibration; raw appends during an open local envelope are rejected,
 * as are local writes on top of raw bytes a rejection left in the buffer, and truncation or recovery reopens a
 * channel a torn raw append left broken. Rejection of invalid raw input itself is pinned by
 * {@link EnvelopeWriteChannelRawRejectionTest}.
 */
@TestDirectoryExtension
class EnvelopeWriteChannelRawStateTrackingTest extends EnvelopeWriteChannelTestSupport {

    @Test
    void stateMirrorsSourceAfterEveryCall() throws IOException {
        int segmentSize = 128;
        // 20 -> FULL; 60 -> BEGIN+END split at the first boundary; 120 -> BEGIN+END at the second; the last entry
        // starts within HEADER_SIZE of the third boundary, so the source pads 29 bytes before its FULL envelope.
        var source = writeSourceEntries(
                segmentSize,
                8,
                new byte[][] {bytes(random, 20), bytes(random, 60), bytes(random, 120), bytes(random, 20)},
                new long[] {T1, T2, T3, T4});

        try (var channel = writeChannel(storeChannel(0), segmentSize, buffer(segmentSize * 8))) {
            for (int i = 0; i < source.entries().size(); i++) {
                var entry = source.entries().get(i);
                channel.appendRaw(chunks(entryChunk(source, i)), entry.index(), entry.term());

                assertChecksum(channel.currentChecksum(), entry.checksum());
                assertThat(channel.currentIndex()).isEqualTo(entry.index());
                assertThat(channel.currentTerm()).isEqualTo(entry.term());
            }
        }

        assertThat(fileBytes(0)).isEqualTo(fileBytes(SOURCE_LOG_VERSION));
    }

    @Test
    void firstAppendOnSeededWriterChainsFromSeedChecksum() throws IOException {
        int segmentSize = 128;
        var source = writeSourceEntries(
                segmentSize, 8, new byte[][] {bytes(random, 20), bytes(random, 24)}, new long[] {T1, T2});
        var firstEntry = source.entries().get(0);
        var secondEntry = source.entries().get(1);

        try (var channel = writeChannel(
                storeChannel(0),
                segmentSize,
                firstEntry.checksum(),
                buffer(segmentSize * 8),
                NO_ROTATION,
                LogTracers.NULL,
                segmentSize,
                firstEntry.index(),
                firstEntry.term())) {
            channel.appendRaw(chunks(entryChunk(source, 1)), secondEntry.index(), secondEntry.term());

            assertChecksum(channel.currentChecksum(), secondEntry.checksum());
            assertThat(channel.currentIndex()).isEqualTo(secondEntry.index());
            assertThat(channel.currentTerm()).isEqualTo(secondEntry.term());
        }
    }

    @Test
    void stateMirrorsKernelVersionAndContentTypeFromRawEnvelopes() throws IOException {
        int segmentSize = 128;
        var source = writeSourceEntries(segmentSize, 8, new byte[][] {bytes(random, 20)}, new long[] {T1});
        var entry = source.entries().get(0);

        try (var channel = writeChannel(storeChannel(0), segmentSize, buffer(segmentSize * 8))) {
            assertThat(channel.currentVersion()).isNotEqualTo(KERNEL_VERSION);
            assertThat(channel.currentContentType()).isNotEqualTo(CONTENT_TYPE);

            channel.appendRaw(chunks(entryChunk(source, 0)), entry.index(), entry.term());

            assertThat(channel.currentVersion()).isEqualTo(KERNEL_VERSION);
            assertThat(channel.currentContentType()).isEqualTo(CONTENT_TYPE);
        }
    }

    @Test
    void bufferWindowWrapKeepsStateInStepWithSource() throws IOException {
        int segmentSize = 128;
        // 91-byte envelopes: every entry after the first crosses a segment boundary as BEGIN+END; the two-segment
        // dest window wraps at 256 and 512 while the eight-segment source never wraps
        var source = writeSourceEntries(
                segmentSize,
                8,
                new byte[][] {bytes(random, 60), bytes(random, 60), bytes(random, 60), bytes(random, 60)},
                new long[] {T1, T2, T3, T4});

        try (var channel = writeChannel(storeChannel(0), segmentSize, buffer(segmentSize * 2))) {
            for (int i = 0; i < source.entries().size(); i++) {
                var entry = source.entries().get(i);
                channel.appendRaw(chunks(entryChunk(source, i)), entry.index(), entry.term());

                assertChecksum(channel.currentChecksum(), entry.checksum());
                assertThat(channel.currentIndex()).isEqualTo(entry.index());
                assertThat(channel.currentTerm()).isEqualTo(entry.term());
            }
        }

        assertThat(fileBytes(0)).isEqualTo(fileBytes(SOURCE_LOG_VERSION));
    }

    @Test
    void singleCallSpanningWindowEdgeParsesEnvelopesBeforeBufferClears() throws IOException {
        int segmentSize = 128;
        // BEGIN + MIDDLE + END in one call through a two-segment window: BEGIN fills exactly up to the window
        // edge, so it must be parsed before the wrap clears the buffer for MIDDLE and END
        var source = writeSourceEntries(
                segmentSize, 8, new byte[][] {bytes(random, (segmentSize - HEADER_SIZE) * 2 + 20)}, new long[] {T1});
        var entry = source.entries().get(0);

        try (var channel = writeChannel(storeChannel(0), segmentSize, buffer(segmentSize * 2))) {
            channel.appendRaw(chunks(entryChunk(source, 0)), entry.index(), entry.term());

            assertChecksum(channel.currentChecksum(), entry.checksum());
        }

        assertThat(fileBytes(0)).isEqualTo(fileBytes(SOURCE_LOG_VERSION));
    }

    @Test
    void foldedPaddingRunEndingAtWindowEdgeIsSkippedBeforeWrap() throws IOException {
        int segmentSize = 128;
        var source = writeDeadZoneSource(segmentSize);
        var entry1 = source.entries().get(0);
        var entry2 = source.entries().get(1);
        // sanity: the dead-zone padding is really in the source, ending exactly at the dest's window edge
        for (int i = entry1.unpaddedEnd(); i < 2 * segmentSize; i++) {
            assertThat(source.data().get(i)).as("source padding byte at %d", i).isZero();
        }
        assertThat(entry2.start()).isEqualTo(2 * segmentSize);
        ByteBuffer foldedChunk = zeroBased(source.data()
                .duplicate()
                .position(entry1.unpaddedEnd())
                .limit(entry2.unpaddedEnd())
                .order(LITTLE_ENDIAN));

        try (var channel = writeChannel(storeChannel(0), segmentSize, buffer(segmentSize * 2))) {
            channel.appendRaw(chunks(entryChunk(source, 0)), entry1.index(), entry1.term());
            channel.appendRaw(chunks(foldedChunk), entry2.index(), entry2.term());

            assertChecksum(channel.currentChecksum(), entry2.checksum());
        }

        assertThat(fileBytes(0)).isEqualTo(fileBytes(SOURCE_LOG_VERSION));
    }

    @Test
    void reopenOnDeadZoneTailRecreatesPaddingAndResumesRaw() throws IOException {
        int segmentSize = 128;
        var source = writeDeadZoneSource(segmentSize);
        var entry1 = source.entries().get(0);
        var entry2 = source.entries().get(1);

        try (var channel = writeChannel(storeChannel(0), segmentSize, buffer(segmentSize * 8))) {
            channel.appendRaw(chunks(entryChunk(source, 0)), entry1.index(), entry1.term());
        } // close flushes: the follower dies with an unpadded dead-zone tail on disk

        try (var reopened = writeChannel(
                storeChannel(0),
                segmentSize,
                entry1.checksum(),
                buffer(segmentSize * 8),
                NO_ROTATION,
                LogTracers.NULL,
                entry1.unpaddedEnd(),
                entry1.index(),
                entry1.term())) {
            // the next shipped range starts at entry 2's envelope: the reopened channel must pad the tail itself
            reopened.appendRaw(chunks(entryChunk(source, 1)), entry2.index(), entry2.term());

            assertChecksum(reopened.currentChecksum(), entry2.checksum());
        }

        byte[] log = fileBytes(0);
        for (int i = entry1.unpaddedEnd(); i < 2 * segmentSize; i++) {
            assertThat(log[i]).as("locally re-created padding byte at %d", i).isZero();
        }
        assertThat(log).isEqualTo(fileBytes(SOURCE_LOG_VERSION));
    }

    @Test
    void directPutAllThenRecoverStateHandsOffToRawWithoutReparsing() throws IOException {
        int segmentSize = 256;
        int dataSize = 25;
        // catchup: entries 1-5 arrive via directPutAll (store copy), entry 6 is then shipped raw
        var part1State = writeLogFileEntries(
                segmentSize, 5, dataSize, new LogContinuityInfo(segmentSize, BASE_TX_ID, BASE_TX_CHECKSUM));
        try (var copyWriter = writeChannel(storeChannel(2L), segmentSize, buffer(segmentSize * 4))) {
            directCopyLogData(segmentSize, BASE_TX_ID, copyWriter);
            copyWriter.recoverState(part1State.lastChecksum(), part1State.lastAppendIndex(), TERM);

            var finalState = writeLogFileEntries(segmentSize, 1, dataSize, part1State);
            var chunk = ByteBuffer.wrap(fileBytes(1L), part1State.offset(), finalState.offset() - part1State.offset());
            // a walk reaching back over the copied bytes would hit envelopes that do not chain from the
            // recovered state, so the append completing at all proves it started above them
            copyWriter.appendRaw(chunks(chunk), finalState.lastAppendIndex(), TERM);

            assertChecksum(copyWriter.currentChecksum(), finalState.lastChecksum());
        }

        assertThat(fileBytes(2L)).isEqualTo(fileBytes(1L));
    }

    @Test
    void recoverStateAfterTornRawAppendReopensChannelForAppends() throws IOException {
        int segmentSize = 128;
        // entry 1 splits BEGIN+END around the second boundary; only its BEGIN envelope arrives raw
        byte[][] payloads = {bytes(random, segmentSize - HEADER_SIZE + 20), bytes(random, 20)};
        var source = writeSourceEntries(segmentSize, 8, payloads, new long[] {T1, T2});
        var entry1 = source.entries().get(0);
        var entry2 = source.entries().get(1);
        ByteBuffer whole = entryChunk(source, 0);
        ByteBuffer beginOnly = whole.duplicate().limit(segmentSize);
        ByteBuffer endEnvelope = whole.duplicate().position(segmentSize);

        try (var channel = writeChannel(storeChannel(0), segmentSize, buffer(segmentSize * 8))) {
            assertThatThrownBy(() -> channel.appendRaw(chunks(beginOnly), entry1.index(), entry1.term()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("torn");

            // catchup repair: the entry's missing END envelope arrives as a copied chunk and the channel is
            // recalibrated; a stale open-entry flag would reject the next local append
            channel.directPutAll(endEnvelope, 2 * segmentSize);
            channel.recoverState(entry1.checksum(), entry1.index(), entry1.term());

            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putTerm(entry2.term());
            channel.putContentType(CONTENT_TYPE);
            channel.putAppendIndex(entry2.index());
            channel.put(payloads[1], payloads[1].length);
            assertChecksum(channel.putChecksum(), entry2.checksum());
            assertThat(channel.currentIndex()).isEqualTo(entry2.index());
        }

        assertThat(fileBytes(0)).isEqualTo(fileBytes(SOURCE_LOG_VERSION));
    }

    @Test
    void truncateToPositionReseedsChainForRawAppends() throws IOException {
        int segmentSize = 128;
        var entries = rawEntries(segmentSize, bytes(random, 20), T1, bytes(random, 24), T2);
        ByteBuffer entryA = zeroBased(entries[0]);
        ByteBuffer entryB = zeroBased(entries[1]);
        int checksumA = entryA.getInt(OFFSET_CHECKSUM);
        int checksumB = entryB.getInt(OFFSET_CHECKSUM);

        var fileChannel = storeChannel(0);
        try (var channel = writeChannel(
                fileChannel,
                segmentSize,
                buffer(segmentSize * 8),
                logRotation(fileChannel, header(segmentSize), segmentSize * 100),
                LogTracers.NULL)) {
            channel.appendRaw(chunks(entryA), FIRST_INDEX, T1);
            channel.appendRaw(chunks(entryB), FIRST_INDEX + 1, T2);
            channel.prepareForFlush();

            // raft truncate: cut back to the end of entry A and rotate onto a fresh file seeded with A's state
            channel.truncateToPosition(segmentSize + entryA.limit(), checksumA, FIRST_INDEX, T1);
            channel.appendRaw(chunks(entryB.duplicate()), FIRST_INDEX + 1, T2);

            assertChecksum(channel.currentChecksum(), checksumB);
            assertThat(channel.currentIndex()).isEqualTo(FIRST_INDEX + 1);
        }

        byte[] envelopeB = new byte[entryB.limit()];
        entryB.duplicate().get(envelopeB);
        assertThat(Arrays.copyOfRange(fileBytes(1), segmentSize, segmentSize + envelopeB.length))
                .as("the rotated file continues with entry B right after its header")
                .isEqualTo(envelopeB);
    }

    @Test
    void truncateToPositionWithWrongSeedRejectsNextRawAppend() throws IOException {
        int segmentSize = 128;
        var entries = rawEntries(segmentSize, bytes(random, 20), T1, bytes(random, 24), T2);
        ByteBuffer entryA = zeroBased(entries[0]);
        ByteBuffer entryB = zeroBased(entries[1]);
        int checksumA = entryA.getInt(OFFSET_CHECKSUM);
        int wrongSeed = checksumA + 1;

        var fileChannel = storeChannel(0);
        try (var channel = writeChannel(
                fileChannel,
                segmentSize,
                buffer(segmentSize * 8),
                logRotation(fileChannel, header(segmentSize), segmentSize * 100),
                LogTracers.NULL)) {
            channel.appendRaw(chunks(entryA), FIRST_INDEX, T1);
            channel.appendRaw(chunks(entryB), FIRST_INDEX + 1, T2);
            channel.prepareForFlush();

            channel.truncateToPosition(segmentSize + entryA.limit(), wrongSeed, FIRST_INDEX, T1);
            assertThatThrownBy(() -> channel.appendRaw(chunks(entryB.duplicate()), FIRST_INDEX + 1, T2))
                    .isInstanceOf(ChecksumMismatchException.class)
                    .hasMessageContaining("checksum chain")
                    .hasMessageContaining("file position " + segmentSize)
                    .hasMessageContaining("index " + (FIRST_INDEX + 1))
                    .hasMessageContaining("term " + T2)
                    .hasMessageContaining(String.valueOf(checksumA))
                    .hasMessageContaining(String.valueOf(wrongSeed));
            assertChecksum(channel.currentChecksum(), wrongSeed);
        }
    }

    @Test
    void truncateAfterTornRawAppendReopensChannelForAppends() throws IOException {
        int segmentSize = 128;
        // entry B splits BEGIN+END around the second boundary; only its BEGIN envelope is ever shipped
        var entries = rawEntries(segmentSize, bytes(random, 20), T1, bytes(random, 60), T2);
        ByteBuffer entryA = zeroBased(entries[0]);
        int checksumA = entryA.getInt(OFFSET_CHECKSUM);
        ByteBuffer tornBegin = zeroBased(entries[1]).limit(2 * segmentSize - (segmentSize + entryA.limit()));
        byte[] payloadC = bytes(random, 24);
        ByteBuffer reference = zeroBased(rawEntry(segmentSize, payloadC, T3, checksumA, FIRST_INDEX, T1));

        var fileChannel = storeChannel(0);
        try (var channel = writeChannel(
                fileChannel,
                segmentSize,
                buffer(segmentSize * 8),
                logRotation(fileChannel, header(segmentSize), segmentSize * 100),
                LogTracers.NULL)) {
            channel.appendRaw(chunks(entryA), FIRST_INDEX, T1);
            assertThatThrownBy(() -> channel.appendRaw(chunks(tornBegin), FIRST_INDEX + 1, T2))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("torn");
            channel.prepareForFlush();

            // raft conflict handling truncates the torn entry away: the channel must be fully reopened,
            // a stale open-entry flag would reject the next local append
            channel.truncateToPosition(segmentSize + entryA.limit(), checksumA, FIRST_INDEX, T1);
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putTerm(T3);
            channel.putContentType(CONTENT_TYPE);
            channel.putAppendIndex(FIRST_INDEX + 1);
            channel.put(payloadC, payloadC.length);
            assertChecksum(channel.putChecksum(), reference.getInt(OFFSET_CHECKSUM));
            assertThat(channel.currentIndex()).isEqualTo(FIRST_INDEX + 1);
        }

        byte[] referenceEnvelope = new byte[reference.limit()];
        reference.duplicate().get(referenceEnvelope);
        assertThat(Arrays.copyOfRange(fileBytes(1), segmentSize, segmentSize + referenceEnvelope.length))
                .as("the rotated file continues with the local entry right after its header")
                .isEqualTo(referenceEnvelope);
    }

    @Test
    void insertStartOffsetThenRawAppendChainsFromSeed() throws IOException {
        int segmentSize = 128;
        int startOffsetSize = 50;
        byte[] payload = bytes(random, 20);
        ByteBuffer envelope = zeroBased(rawEntry(segmentSize, payload, T1));
        int envelopeChecksum = envelope.getInt(OFFSET_CHECKSUM);

        try (var channel = writeChannel(storeChannel(0), segmentSize, buffer(segmentSize * 8))) {
            channel.insertStartOffset(startOffsetSize);
            // START_OFFSET envelopes do not participate in the chain: the raw envelope still chains from the seed
            channel.appendRaw(chunks(envelope), FIRST_INDEX, T1);

            assertChecksum(channel.currentChecksum(), envelopeChecksum);
            assertThat(channel.currentIndex()).isEqualTo(FIRST_INDEX);
        }

        var data = ByteBuffer.wrap(fileBytes(0)).order(LITTLE_ENDIAN).position(segmentSize);
        assertEnvelopeContents(
                data,
                startOffset(startOffsetSize - HEADER_SIZE),
                envelope(FULL, FIRST_INDEX, payload, KERNEL_VERSION, envelopeChecksum, T1, CONTENT_TYPE));
    }

    @Test
    void pojoAppendAfterCleanRawEntriesMatchesAllPojoLog() throws IOException {
        int segmentSize = 128;
        // 46 makes entry 2 end exactly on the second segment boundary, so the handoff also copes with a boundary tail
        byte[][] payloads = {bytes(random, 20), bytes(random, 46), bytes(random, 20)};
        var source = writeSourceEntries(segmentSize, 8, payloads, new long[] {T1, T2, T3});
        var entry1 = source.entries().get(0);
        var entry2 = source.entries().get(1);
        var entry3 = source.entries().get(2);
        assertThat(entry2.unpaddedEnd()).isEqualTo(2 * segmentSize);

        try (var channel = writeChannel(storeChannel(0), segmentSize, buffer(segmentSize * 8))) {
            channel.appendRaw(chunks(entryChunk(source, 0)), entry1.index(), entry1.term());
            channel.appendRaw(chunks(entryChunk(source, 1)), entry2.index(), entry2.term());

            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putTerm(entry3.term());
            channel.putContentType(CONTENT_TYPE);
            channel.putAppendIndex(entry3.index());
            channel.put(payloads[2], payloads[2].length);
            assertChecksum(channel.putChecksum(), entry3.checksum());
            assertThat(channel.currentIndex()).isEqualTo(entry3.index());
        }

        assertThat(fileBytes(0)).isEqualTo(fileBytes(SOURCE_LOG_VERSION));
    }

    @Test
    void rawAppendAfterPojoEntryResumesChain() throws IOException {
        int segmentSize = 128;
        // 60 makes entry 2 cross the first segment boundary as BEGIN+END, so the resumed chain spans two envelopes
        byte[][] payloads = {bytes(random, 20), bytes(random, 60), bytes(random, 24)};
        var source = writeSourceEntries(segmentSize, 8, payloads, new long[] {T1, T2, T3});
        var entry1 = source.entries().get(0);
        var entry2 = source.entries().get(1);
        var entry3 = source.entries().get(2);

        try (var channel = writeChannel(storeChannel(0), segmentSize, buffer(segmentSize * 8))) {
            writeEntry(channel, payloads[0], T1);
            assertChecksum(channel.currentChecksum(), entry1.checksum());

            channel.appendRaw(chunks(entryChunk(source, 1)), entry2.index(), entry2.term());
            channel.appendRaw(chunks(entryChunk(source, 2)), entry3.index(), entry3.term());

            assertChecksum(channel.currentChecksum(), entry3.checksum());
            assertThat(channel.currentIndex()).isEqualTo(entry3.index());
            assertThat(channel.currentTerm()).isEqualTo(entry3.term());
        }

        assertThat(fileBytes(0)).isEqualTo(fileBytes(SOURCE_LOG_VERSION));
    }

    @Test
    void multiSegmentEntryDeliveredAsPerSegmentChunksInOneCall() throws IOException {
        int segmentSize = 128;
        // BEGIN + MIDDLE + END: the production decoder cuts one chunk per segment and delivers them in one call
        var source = writeSourceEntries(
                segmentSize, 8, new byte[][] {bytes(random, (segmentSize - HEADER_SIZE) * 2 + 20)}, new long[] {T1});
        var entry = source.entries().get(0);
        ByteBuffer whole = entryChunk(source, 0);
        ByteBuffer beginEnvelope = whole.duplicate().limit(segmentSize);
        ByteBuffer middleEnvelope = whole.duplicate().position(segmentSize).limit(2 * segmentSize);
        ByteBuffer endEnvelope = whole.duplicate().position(2 * segmentSize);

        try (var channel = writeChannel(storeChannel(0), segmentSize, buffer(segmentSize * 8))) {
            channel.appendRaw(chunks(beginEnvelope, middleEnvelope, endEnvelope), entry.index(), entry.term());

            assertChecksum(channel.currentChecksum(), entry.checksum());
            assertThat(channel.currentIndex()).isEqualTo(entry.index());
            assertThat(channel.currentTerm()).isEqualTo(entry.term());
        }

        assertThat(fileBytes(0)).isEqualTo(fileBytes(SOURCE_LOG_VERSION));
    }

    @Test
    void emptyChunkMixedWithEnvelopeChunksIsSkipped() throws IOException {
        int segmentSize = 128;
        // an empty chunk between two envelope chunks of one entry must be skipped, not end the call: stopping
        // there would leave the entry open and reject the whole append
        var source = writeSourceEntries(
                segmentSize, 8, new byte[][] {bytes(random, segmentSize - HEADER_SIZE + 20)}, new long[] {T1});
        var entry = source.entries().get(0);
        ByteBuffer whole = entryChunk(source, 0);
        ByteBuffer beginEnvelope = whole.duplicate().limit(segmentSize);
        ByteBuffer endEnvelope = whole.duplicate().position(segmentSize);

        try (var channel = writeChannel(storeChannel(0), segmentSize, buffer(segmentSize * 8))) {
            channel.appendRaw(chunks(beginEnvelope, ByteBuffer.allocate(0), endEnvelope), entry.index(), entry.term());

            assertChecksum(channel.currentChecksum(), entry.checksum());
        }

        assertThat(fileBytes(0)).isEqualTo(fileBytes(SOURCE_LOG_VERSION));
    }

    @Test
    void rawAppendDuringOpenLocalEnvelopeThrows() throws IOException {
        int segmentSize = 128;
        byte[] localPayload = bytes(random, 20);
        // what the entry looks like written locally on a fresh chain: the expected result after completion
        int referenceChecksum =
                zeroBased(rawEntry(segmentSize, localPayload, T1)).getInt(OFFSET_CHECKSUM);
        ByteBuffer envelope =
                zeroBased(rawEntry(segmentSize, bytes(random, 24), T2, referenceChecksum, FIRST_INDEX, T1));
        int envelopeChecksum = envelope.getInt(OFFSET_CHECKSUM);

        try (var channel = writeChannel(storeChannel(0), segmentSize, buffer(segmentSize * 8))) {
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putTerm(T1);
            channel.putContentType(CONTENT_TYPE);
            channel.putAppendIndex(FIRST_INDEX);
            channel.put(localPayload, localPayload.length);

            // raw bytes would land inside the open payload and the walk would parse the unwritten header region
            assertThatThrownBy(() -> channel.appendRaw(chunks(envelope), FIRST_INDEX + 1, T2))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("still open")
                    .hasMessageContaining((HEADER_SIZE + localPayload.length) + " bytes")
                    .hasMessageContaining("file position " + segmentSize);

            // the guard must leave the open envelope untouched: it completes and raw shipping resumes after it
            assertChecksum(channel.putChecksum(), referenceChecksum);
            channel.appendRaw(chunks(envelope), FIRST_INDEX + 1, T2);
            assertChecksum(channel.currentChecksum(), envelopeChecksum);
        }
    }

    @Test
    void pojoAppendAfterRejectedRawBytesThrows() throws IOException {
        int segmentSize = 128;
        ByteBuffer envelope = zeroBased(rawEntry(segmentSize, bytes(random, 20), T1));
        int doctored = 0xBADC0DE;
        envelope.putInt(OFFSET_PREVIOUS_CHECKSUM, doctored);
        envelope.putInt(OFFSET_CHECKSUM, envelopeCrc(envelope));

        try (var channel = writeChannel(storeChannel(0), segmentSize, buffer(segmentSize * 8))) {
            assertThatThrownBy(() -> channel.appendRaw(chunks(envelope), FIRST_INDEX, T1))
                    .isInstanceOf(ChecksumMismatchException.class);

            // the rejected envelope's bytes sit in the buffer above the last completed envelope: a local write must
            // not start on top of them and flush them into the log
            assertThatThrownBy(channel::beginChecksumForWriting)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("still open")
                    .hasMessageContaining((HEADER_SIZE + 20) + " bytes")
                    .hasMessageContaining("file position " + segmentSize);
        }
    }

    @Test
    void entryEndingExactlyOnSegmentBoundaryHandsOverCleanly() throws IOException {
        int segmentSize = 128;
        // payload 97 = segmentSize - HEADER_SIZE: the envelope fills its segment exactly, which also pins the
        // maximum legal payload length as accepted
        var source = writeSourceEntries(
                segmentSize, 8, new byte[][] {bytes(random, segmentSize - HEADER_SIZE), bytes(random, 20)}, new long[] {
                    T1, T2
                });
        var entry1 = source.entries().get(0);
        var entry2 = source.entries().get(1);
        assertThat(entry1.unpaddedEnd()).isEqualTo(2 * segmentSize);

        try (var channel = writeChannel(storeChannel(0), segmentSize, buffer(segmentSize * 8))) {
            channel.appendRaw(chunks(entryChunk(source, 0)), entry1.index(), entry1.term());
            assertChecksum(channel.currentChecksum(), entry1.checksum());

            channel.appendRaw(chunks(entryChunk(source, 1)), entry2.index(), entry2.term());
            assertChecksum(channel.currentChecksum(), entry2.checksum());
        }

        assertThat(fileBytes(0)).isEqualTo(fileBytes(SOURCE_LOG_VERSION));
    }
}
