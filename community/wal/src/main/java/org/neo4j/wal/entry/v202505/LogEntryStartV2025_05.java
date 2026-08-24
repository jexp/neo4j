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
package org.neo4j.wal.entry.v202505;

import java.util.Arrays;
import java.util.Objects;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.string.Mask;
import org.neo4j.wal.entry.LogEntryStart;

public class LogEntryStartV2025_05 extends LogEntryStart {
    protected final long appendIndex;

    public LogEntryStartV2025_05(
            KernelVersion kernelVersion,
            long timeWritten,
            long lastCommittedTxWhenTransactionStarted,
            long appendIndex,
            byte[] additionalHeader) {
        super(kernelVersion, timeWritten, lastCommittedTxWhenTransactionStarted, additionalHeader);
        this.appendIndex = appendIndex;
    }

    @Override
    public long getAppendIndex() {
        return appendIndex;
    }

    @Override
    public String toString(Mask mask) {
        return "LogEntryStartV2025_05[" + "kernelVersion=" + kernelVersion() + ",time=" + timestamp(timeWritten)
                + ",lastCommittedTxWhenTransactionStarted=" + lastCommittedTxWhenTransactionStarted
                + ",additionalHeaderLength=" + (additionalHeader == null ? -1 : additionalHeader.length) + ","
                + (additionalHeader == null ? "" : Arrays.toString(additionalHeader))
                + ", appendIndex=" + appendIndex + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogEntryStartV2025_05 start = (LogEntryStartV2025_05) o;
        return lastCommittedTxWhenTransactionStarted == start.lastCommittedTxWhenTransactionStarted
                && timeWritten == start.timeWritten
                && kernelVersion() == start.kernelVersion()
                && Arrays.equals(additionalHeader, start.additionalHeader)
                && appendIndex == start.appendIndex;
    }

    @Override
    public int hashCode() {
        int result = (int) (timeWritten ^ (timeWritten >>> 32));
        result = 31 * result
                + (int) (lastCommittedTxWhenTransactionStarted ^ (lastCommittedTxWhenTransactionStarted >>> 32));
        result = 31 * result + (additionalHeader != null ? Arrays.hashCode(additionalHeader) : 0);
        return Objects.hash(result, appendIndex);
    }
}
