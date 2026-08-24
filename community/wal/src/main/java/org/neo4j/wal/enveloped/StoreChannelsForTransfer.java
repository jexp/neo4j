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

import java.io.IOException;
import java.util.List;
import org.neo4j.io.fs.StoreChannel;

/**
 * Contains a list of {@link StoreChannel} for transfer reflecting the range of bytes for the given index range in the
 * log.
 * @param storeChannels    a list of {@link StoreChannel} each channel is positioned at their start and end positions
 *                         for that file's log range. The first channel start is positioned at {@code fromIndex};
 *                         follow-on channels sit at their first entry byte (any leading START_OFFSET filler skipped),
 *                         with {@link #segmentOffset(int)} as their intra-segment grid offset. A consumer that wants
 *                         the physical layout instead rewinds with {@link #rewindFollowOnChannelsToSegmentStart()}.
 * @param toPosition       reflects the end position of {@code toIndex}. This is always for the last channel in the
 *                         list.
 * @param fromIndex        first index in the range
 * @param toIndex          last index in the range
 * @param segmentBlockSize size of one envelope segment in bytes; {@code 0} when the transfer is empty.
 */
public record StoreChannelsForTransfer(
        List<StoreChannel> storeChannels, long toPosition, long fromIndex, long toIndex, int segmentBlockSize) {

    public static StoreChannelsForTransfer nothingToTransfer(long fromIndex, long toIndex) {
        return new StoreChannelsForTransfer(List.of(), -1, fromIndex, toIndex, 0);
    }

    /**
     * The intra-segment offset of the first byte channel {@code i} will deliver. Files are segment-aligned from
     * byte 0, so the offset is the position's remainder.
     */
    public int segmentOffset(int i) throws IOException {
        return segmentBlockSize == 0 ? 0 : (int) (storeChannels.get(i).position() % segmentBlockSize);
    }

    /**
     * Repositions every follow-on channel back to the start of its file's first data segment, undoing the skip of
     * any leading START_OFFSET filler so the delivered bytes reproduce the physical file layout. The first channel
     * is never moved — it starts mid-file at {@code fromIndex}, not at a file boundary.
     */
    public void rewindFollowOnChannelsToSegmentStart() throws IOException {
        for (int i = 1; i < storeChannels.size(); i++) {
            var channel = storeChannels.get(i);
            channel.position(channel.position() - segmentOffset(i));
        }
    }
}
