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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.wal.entry.LogEnvelopeHeader.EnvelopeType.FULL;
import static org.neo4j.wal.entry.LogEnvelopeHeader.HEADER_SIZE;
import static org.neo4j.wal.entry.LogEnvelopeHeader.MAX_ZERO_PADDING_SIZE;
import static org.neo4j.wal.entry.LogEnvelopeHeader.OFFSET_CHECKSUM;
import static org.neo4j.wal.entry.LogEnvelopeHeader.OFFSET_PREVIOUS_CHECKSUM;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;
import org.neo4j.io.fs.ChecksumMismatchException;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.wal.LogTracers;

/**
 * Focused coverage for {@link EnvelopeWriteChannel#appendRaw(ByteBuffer[], long, long)}, exercising each control-flow
 * state: no boundary crossing, padding-only, rotation during the write, and rotation before the write. The last two
 * pin the index/term timing: those are set only after the first chunk's (pre-write) rotation check, so a rotation at
 * an entry start carries the previous entry's index/term while a rotation mid-entry carries the current entry's. The
 * rotated file's header is random test bytes, so we assert on the boundary state the channel seeded into the rotation
 * (captured by {@link EnvelopeWriteChannelTestSupport#logRotation}).
 */
@TestDirectoryExtension
class EnvelopeWriteChannelRawAppendTest extends EnvelopeWriteChannelTestSupport {

    @Test
    void smallEntryDoesNotCrossSegmentAndDoesNotRotate() throws IOException {
        int segmentSize = 256;
        ByteBuffer entry = rawEntry(segmentSize, bytes(random, 32), T1);

        appendOne(segmentSize, segmentSize * 4, entry, FIRST_INDEX, T1);

        assertThat(fileSystem.fileExists(logPath(1)))
                .as("no rotation for a small entry")
                .isFalse();
        assertThat(lastRotatedAppendIndex).as("no rotation occurred").isEqualTo(-1L);
    }

    @Test
    void entrySpanningSegmentPadsButDoesNotRotateUnderLimit() throws IOException {
        int segmentSize = 128;
        // BEGIN + END across two segments, comfortably under an 8-segment limit
        ByteBuffer entry = rawEntry(segmentSize, bytes(random, (segmentSize - HEADER_SIZE) + (segmentSize / 4)), T1);

        appendOne(segmentSize, segmentSize * 8, entry, FIRST_INDEX, T1);

        assertThat(lastRotatedAppendIndex)
                .as("no rotation under the size limit")
                .isEqualTo(-1L);
        assertThat(fileSystem.getFileSize(logPath(0)))
                .as("a segment boundary was crossed, so the entry spans more than one data segment")
                .isGreaterThan((long) segmentSize * 2);
    }

    @Test
    void rotationDuringWriteCarriesCurrentEntryIndexAndTerm() throws IOException {
        int segmentSize = 128;
        // three data segments -> crosses the limit mid-entry (MIDDLE/END continuation)
        ByteBuffer entry = rawEntry(segmentSize, bytes(random, (segmentSize - HEADER_SIZE) * 3), T1);

        appendOne(segmentSize, segmentSize * 3, entry, FIRST_INDEX, T1); // limit = header + 2 data segments

        assertThat(fileSystem.fileExists(logPath(1))).as("rotated mid-entry").isTrue();
        assertThat(lastRotatedAppendIndex)
                .as("MIDDLE/END continuation keeps the current entry's index")
                .isEqualTo(FIRST_INDEX);
        assertThat(lastRotatedTerm).isEqualTo(T1);
    }

    @Test
    void rotationBeforeWriteCarriesPreviousEntryIndexAndTerm() throws IOException {
        int segmentSize = 128;
        // entryA is a single FULL segment, so the channel sits exactly on the boundary at the limit after it
        // without entryA itself rotating; entryB then rotates in the pre-write check at its BEGIN.
        ByteBuffer[] both =
                rawEntries(segmentSize, bytes(random, segmentSize - HEADER_SIZE), T1, bytes(random, 32), T2);

        var fileChannel = storeChannel(0);
        try (var channel = writeChannel(
                fileChannel,
                segmentSize,
                buffer(segmentSize * 6),
                logRotation(fileChannel, header(segmentSize), segmentSize * 2), // header + 1 data segment
                LogTracers.NULL)) {
            channel.appendRaw(chunks(both[0]), FIRST_INDEX, T1);
            assertThat(lastRotatedAppendIndex)
                    .as("entryA fills to the limit but does not rotate yet")
                    .isEqualTo(-1L);
            channel.appendRaw(chunks(both[1]), FIRST_INDEX + 1, T2);
        }

        assertThat(fileSystem.fileExists(logPath(1)))
                .as("rotated at entryB's start")
                .isTrue();
        assertThat(lastRotatedAppendIndex)
                .as("BEGIN at a boundary -> the rotation carries the PREVIOUS entry's index, not entryB's")
                .isEqualTo(FIRST_INDEX);
        assertThat(lastRotatedTerm)
                .as("...and the previous entry's term (catches the term lag)")
                .isEqualTo(T1);
    }

    @Test
    void rotationBeforeRawWriteSeedsTrackedChecksum() throws IOException {
        int segmentSize = 128;
        // entryA fills exactly to the rotation limit; entryB's append rotates in the pre-write check, and the
        // rotation must be seeded with entryA's checksum
        ByteBuffer[] both =
                rawEntries(segmentSize, bytes(random, segmentSize - HEADER_SIZE), T1, bytes(random, 32), T2);
        int entryAChecksum = both[0].getInt(both[0].position() + OFFSET_CHECKSUM);

        var fileChannel = storeChannel(0);
        try (var channel = writeChannel(
                fileChannel,
                segmentSize,
                buffer(segmentSize * 6),
                logRotation(fileChannel, header(segmentSize), segmentSize * 2),
                LogTracers.NULL)) {
            channel.appendRaw(chunks(both[0]), FIRST_INDEX, T1);
            channel.appendRaw(chunks(both[1]), FIRST_INDEX + 1, T2);
        }

        assertThat(fileSystem.fileExists(logPath(1)))
                .as("rotated at entryB's start")
                .isTrue();
        assertChecksum(lastRotatedPreviousChecksum, entryAChecksum);
    }

    @Test
    void rejectsForgedPreviousChecksumAtRotationBoundary() throws IOException {
        // The deleted rotation peek adopted entryB's previous-checksum field into the tracked chain, so a forgery
        // placed exactly at a rotation boundary self-compared and was silently seeded into the rotated file.
        int segmentSize = 128;
        ByteBuffer[] both =
                rawEntries(segmentSize, bytes(random, segmentSize - HEADER_SIZE), T1, bytes(random, 32), T2);
        int entryAChecksum = both[0].getInt(both[0].position() + OFFSET_CHECKSUM);
        ByteBuffer entryB = zeroBased(both[1]);
        int forged = 0xBADC0DE;
        entryB.putInt(OFFSET_PREVIOUS_CHECKSUM, forged);
        entryB.putInt(OFFSET_CHECKSUM, envelopeCrc(entryB));

        var fileChannel = storeChannel(0);
        try (var channel = writeChannel(
                fileChannel,
                segmentSize,
                buffer(segmentSize * 6),
                logRotation(fileChannel, header(segmentSize), segmentSize * 2),
                LogTracers.NULL)) {
            channel.appendRaw(chunks(both[0]), FIRST_INDEX, T1);
            assertThatThrownBy(() -> channel.appendRaw(chunks(entryB), FIRST_INDEX + 1, T2))
                    .isInstanceOf(ChecksumMismatchException.class)
                    .hasMessageContaining("checksum chain")
                    .hasMessageContaining("file position " + segmentSize)
                    .hasMessageContaining("index " + (FIRST_INDEX + 1))
                    .hasMessageContaining("term " + T2)
                    .hasMessageContaining(String.valueOf(forged))
                    .hasMessageContaining(String.valueOf(entryAChecksum));
        }

        // the rotation fired before the forged envelope was parsed, seeded from the tracked chain, not the forgery
        assertChecksum(lastRotatedPreviousChecksum, entryAChecksum);
    }

    @Test
    void rawAppendPadsTailLargerThanHeaderBeforeFullEnvelope() throws IOException {
        // A follower left mid-segment on a tail in (HEADER_SIZE, MAX_ZERO_PADDING_SIZE] must pad it before the next
        // envelope: the old guard only padded tail <= HEADER_SIZE, so such an envelope straddled the boundary.
        int segmentSize = 128;
        int tail = HEADER_SIZE + 1; // 32: > HEADER_SIZE (31), <= MAX_ZERO_PADDING_SIZE (39) -> the dead-zone
        assertThat(tail).isGreaterThan(HEADER_SIZE).isLessThanOrEqualTo(MAX_ZERO_PADDING_SIZE);

        // Entry A: a FULL envelope that ends exactly `tail` bytes short of the segment boundary, leaving the follower
        // mid-segment (the divergence a restart/store-copy or an omitted last-entry pad produces in prod).
        int envelopeA = segmentSize - tail; // 96
        ByteBuffer entryA = zeroBased(rawEntry(segmentSize, bytes(random, envelopeA - HEADER_SIZE), T1));
        // Entry B, shipped from a segment-aligned leader: a clean FULL envelope larger than `tail`, chained after A.
        ByteBuffer entryB = zeroBased(
                rawEntry(segmentSize, bytes(random, 40), T2, entryA.getInt(OFFSET_CHECKSUM), FIRST_INDEX, T1));

        var fileChannel = storeChannel(0);
        try (var channel = writeChannel(fileChannel, segmentSize, buffer(segmentSize * 8))) {
            channel.appendRaw(chunks(entryA), FIRST_INDEX, T1); // follower now sits `tail` bytes from the boundary
            channel.appendRaw(chunks(entryB), FIRST_INDEX + 1, T2); // must NOT throw: pads to the boundary first
        }

        byte[] log = fileBytes(0);
        // The tail after entry A (up to the next segment boundary) must be zero padding...
        for (int i = segmentSize + envelopeA; i < 2 * segmentSize; i++) {
            assertThat(log[i]).as("padding byte at offset %d", i).isZero();
        }
        // ...and entry B's FULL envelope must begin exactly on that boundary (type byte after the 4-byte checksum).
        assertThat(log[2 * segmentSize + Integer.BYTES])
                .as("entry B type byte at the segment boundary")
                .isEqualTo(FULL.typeValue);
    }

    @Test
    void rawAppendDoesNotPadWhenEnvelopeFillsTailExactly() throws IOException {
        // Strict '>' boundary: an envelope whose total length equals the remaining tail (a BEGIN split, or an exact-fit
        // FULL) is what the leader wrote contiguously in that tail — the follower must place it there, not pad.
        int segmentSize = 128;
        int tail = MAX_ZERO_PADDING_SIZE; // 39
        int envelopeA = segmentSize - tail; // 89 -> leaves a 39-byte tail
        ByteBuffer entryA = zeroBased(rawEntry(segmentSize, bytes(random, envelopeA - HEADER_SIZE), T1));
        // Entry B is a FULL envelope of exactly `tail` bytes (HEADER_SIZE + 8), filling the tail to the boundary,
        // chained after A as the leader would have written it.
        ByteBuffer entryB = zeroBased(rawEntry(
                segmentSize, bytes(random, tail - HEADER_SIZE), T2, entryA.getInt(OFFSET_CHECKSUM), FIRST_INDEX, T1));

        var fileChannel = storeChannel(0);
        try (var channel = writeChannel(fileChannel, segmentSize, buffer(segmentSize * 8))) {
            channel.appendRaw(chunks(entryA), FIRST_INDEX, T1);
            channel.appendRaw(chunks(entryB), FIRST_INDEX + 1, T2); // exact fit -> written contiguously, no pad
        }

        byte[] log = fileBytes(0);
        // Entry B sits immediately after entry A (contiguous), not pushed to the boundary; no padding inserted.
        assertThat(log[segmentSize + envelopeA + Integer.BYTES])
                .as("entry B type byte, written contiguously after entry A")
                .isEqualTo(FULL.typeValue);
    }

    // ---- helpers ----

    private void appendOne(int segmentSize, long maxFileSize, ByteBuffer entry, long index, long term)
            throws IOException {
        var fileChannel = storeChannel(0);
        try (var channel = writeChannel(
                fileChannel,
                segmentSize,
                buffer(segmentSize * 6),
                logRotation(fileChannel, header(segmentSize), maxFileSize),
                LogTracers.NULL)) {
            channel.appendRaw(chunks(entry), index, term);
        }
    }
}
