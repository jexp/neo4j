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
package org.neo4j.wal.entry;

import static org.neo4j.wal.entry.LogEntryTypeCodes.EMPTY_TX;

import org.neo4j.kernel.KernelVersion;
import org.neo4j.string.Mask;

/**
 * Log Entry used to signal that there is something in the log that should be considered a tx but
 * the content is irrelevant.
 */
public class LogEntryEmpty extends AbstractVersionAwareLogEntry {
    private final long appendIndex;
    private final byte contentType;

    public LogEntryEmpty(long appendIndex, KernelVersion kernelVersion, byte contentType) {
        super(kernelVersion, EMPTY_TX);
        this.appendIndex = appendIndex;
        this.contentType = contentType;
    }

    @Override
    public String toString(Mask mask) {
        return "LogEntryEmpty[" + "kernelVersion=" + kernelVersion() + ", appendIndex=" + appendIndex + ", contentType="
                + contentType
                + ". NOTE this is not an actual entry, but injected instead of non-tx entries when reading the log]";
    }

    public long getAppendIndex() {
        return appendIndex;
    }

    public byte getContentType() {
        return contentType;
    }
}
