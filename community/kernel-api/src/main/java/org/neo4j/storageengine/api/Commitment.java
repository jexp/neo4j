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
package org.neo4j.storageengine.api;

import org.neo4j.kernel.KernelVersion;
import org.neo4j.wal.LogPosition;

/**
 * Represents a commitment that is result of applying batch of commands to transaction logs
 * means. As a transaction is carried through the commit process this commitment is updated
 * when {@link #publishAsCommitted(long, long)} committed (which happens when appending to log), but also
 * when {@link #publishAsClosed()} closing.
 */
public interface Commitment {
    Commitment NO_COMMITMENT = new Commitment() {

        @Override
        public void commit(
                long transactionId,
                long appendIndex,
                boolean firstBatch,
                boolean lastBatch,
                KernelVersion kernelVersion,
                LogPosition logPositionBeforeCommit,
                LogPosition logPositionAfterCommit,
                int checksum,
                long consensusIndex) {}

        @Override
        public void publishAsCommitedLastBatch() {}

        @Override
        public void publishEmptyAsCommitted(long transactionCommitTimestamp) {}

        @Override
        public void publishAsCommitted(long transactionCommitTimestamp, long firstAppendIndex) {}

        @Override
        public void publishEmptyAsClosed() {}

        @Override
        public void publishAsClosed() {}
    };

    void commit(
            long transactionId,
            long appendIndex,
            boolean firstBatch,
            boolean lastBatch,
            KernelVersion kernelVersion,
            LogPosition logPositionBeforeCommit,
            LogPosition logPositionAfterCommit,
            int checksum,
            long consensusIndex);

    /**
     * Commit the last appended transaction batch.
     */
    void publishAsCommitedLastBatch();

    /**
     * Marks non-first chunks of multi-chunked transactions as committed.
     *
     * <p>The chunks in multi-chunked transactions all share a transaction id. On merged logs, this can result in gaps,
     * as that txid is based on the append index of the first chunk. We must fill those gaps by "publishing" the commit
     * of each chunk.
     */
    void publishEmptyAsCommitted(long transactionCommitTimestamp);

    /**
     * Marks the transaction as committed and makes this fact public.
     */
    void publishAsCommitted(long transactionCommitTimestamp, long firstAppendIndex);

    /**
     * Marks non-first chunks of multi-chunked transactions as closed.
     *
     * <p>The chunks in multi-chunked transactions all share a transaction id. On merged logs, this can result in gaps,
     * as that txid is based on the append index of the first chunk. We must fill those gaps by "publishing" the closing
     * of each chunk.
     */
    void publishEmptyAsClosed();

    /**
     * Marks the transaction as closed and makes this fact public.
     */
    void publishAsClosed();
}
