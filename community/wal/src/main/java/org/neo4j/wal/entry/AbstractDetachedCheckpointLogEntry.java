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

import static org.neo4j.wal.entry.LogEntryTypeCodes.DETACHED_CHECK_POINT_V5_0;

import org.neo4j.kernel.KernelVersion;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.string.Mask;
import org.neo4j.wal.LogPosition;

public abstract class AbstractDetachedCheckpointLogEntry extends AbstractVersionAwareLogEntry {
    protected final LogPosition checkpointedLogPosition;
    protected final long checkpointTime;
    protected final StoreId storeId;
    protected final String reason;

    public AbstractDetachedCheckpointLogEntry(
            KernelVersion kernelVersion,
            LogPosition checkpointedLogPosition,
            long checkpointMillis,
            StoreId storeId,
            String reason) {
        this(kernelVersion, checkpointedLogPosition, checkpointMillis, storeId, reason, DETACHED_CHECK_POINT_V5_0);
    }

    public AbstractDetachedCheckpointLogEntry(
            KernelVersion kernelVersion,
            LogPosition checkpointedLogPosition,
            long checkpointMillis,
            StoreId storeId,
            String reason,
            byte type) {
        super(kernelVersion, type);
        this.checkpointedLogPosition = checkpointedLogPosition;
        this.checkpointTime = checkpointMillis;
        this.storeId = storeId;
        this.reason = reason;
    }

    public StoreId getStoreId() {
        return storeId;
    }

    public LogPosition getCheckpointedLogPosition() {
        return checkpointedLogPosition;
    }

    public String getReason() {
        return reason;
    }

    public long getCheckpointTime() {
        return checkpointTime;
    }

    @Override
    public abstract String toString(Mask mask);

    @Override
    public abstract int hashCode();

    @Override
    public abstract boolean equals(Object obj);
}
