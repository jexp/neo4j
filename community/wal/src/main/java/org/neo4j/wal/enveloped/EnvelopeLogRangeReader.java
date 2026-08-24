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

public interface EnvelopeLogRangeReader {

    /**
     * Prepares all the relevant files to be transferred in a {@link StoreChannelsForTransfer}.
     * Every channel is positioned at its first entry byte: the first file at the start of {@code fromIndex}, the
     * others past their file header and any leading START_OFFSET filler, so local layout never reaches the consumer.
     * Each channel's intra-segment grid offset is {@link StoreChannelsForTransfer#segmentOffset(int)}.
     * @param fromIndex start of range. This is required to start the range. Pass -1 for earliest available.
     * @param desiredToIndex desired end of range. This is not required to be the end of the range. If the log
     *                       cannot serve this index it will still return a lower index.
     * @return  Returns a {@link StoreChannelsForTransfer} for the given {@code fromIndex} and {@code desiredToIndex}.
     * @throws IOException
     */
    StoreChannelsForTransfer entryStreamChannels(long fromIndex, long desiredToIndex) throws IOException;

    long term(long index) throws IOException;

    long highestReadableIndex();
}
