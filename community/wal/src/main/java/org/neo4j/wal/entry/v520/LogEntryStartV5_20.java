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
package org.neo4j.wal.entry.v520;

import java.util.Arrays;
import java.util.Objects;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.string.Mask;
import org.neo4j.wal.entry.LogEntryStart;

public class LogEntryStartV5_20 extends LogEntryStart {
    private final long appendIndex;
    private final int previousChecksum;

    public LogEntryStartV5_20(
            KernelVersion kernelVersion,
            long timeWritten,
            long lastCommittedTxWhenTransactionStarted,
            long appendIndex,
            int previousChecksum,
            byte[] additionalHeader) {
        super(kernelVersion, timeWritten, lastCommittedTxWhenTransactionStarted, additionalHeader);
        this.previousChecksum = previousChecksum;
        this.appendIndex = appendIndex;
    }

    public int getPreviousChecksum() {
        return previousChecksum;
    }

    @Override
    public long getAppendIndex() {
        return appendIndex;
    }

    @Override
    public String toString(Mask mask) {
        return "LogEntryStartV5_20[" + "kernelVersion=" + kernelVersion() + ",time=" + timestamp(timeWritten)
                + ",lastCommittedTxWhenTransactionStarted=" + lastCommittedTxWhenTransactionStarted
                + ",additionalHeaderLength=" + (additionalHeader == null ? -1 : additionalHeader.length) + ","
                + (additionalHeader == null ? "" : Arrays.toString(additionalHeader))
                + ", appendIndex="
                + appendIndex + ", previousChecksum=" + previousChecksum + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogEntryStartV5_20 start = (LogEntryStartV5_20) o;
        return lastCommittedTxWhenTransactionStarted == start.lastCommittedTxWhenTransactionStarted
                && timeWritten == start.timeWritten
                && kernelVersion() == start.kernelVersion()
                && Arrays.equals(additionalHeader, start.additionalHeader)
                && appendIndex == start.appendIndex
                && previousChecksum == start.previousChecksum;
    }

    @Override
    public int hashCode() {
        int result = (int) (timeWritten ^ (timeWritten >>> 32));
        result = 31 * result
                + (int) (lastCommittedTxWhenTransactionStarted ^ (lastCommittedTxWhenTransactionStarted >>> 32));
        result = 31 * result + (additionalHeader != null ? Arrays.hashCode(additionalHeader) : 0);
        return Objects.hash(result, appendIndex, previousChecksum);
    }
}
