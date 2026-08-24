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
package org.neo4j.wal;

import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CHUNK_ID;

import java.io.IOException;
import java.util.Iterator;
import org.neo4j.common.Subject;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.api.LeaseService;
import org.neo4j.storageengine.api.CommandBatch;
import org.neo4j.storageengine.api.Leases;
import org.neo4j.storageengine.api.StorageCommand;

public record EmptyCommandBatch(KernelVersion kernelVersion, long appendIndex) implements CommandBatch {
    @Override
    public long consensusIndex() {
        return appendIndex;
    }

    @Override
    public void setConsensusIndex(long commandIndex) {}

    @Override
    public long getTimeStarted() {
        return 0;
    }

    @Override
    public long getLatestCommittedTxWhenStarted() {
        return 0;
    }

    @Override
    public long getTimeCommitted() {
        return 0;
    }

    @Override
    public int getLeaseId() {
        return LeaseService.NO_LEASE;
    }

    @Override
    public Leases leases() {
        return Leases.NO_LEASES;
    }

    @Override
    public Subject subject() {
        return null;
    }

    @Override
    public String toString(boolean includeCommands) {
        return "Empty command batch. KernelVersion " + kernelVersion + ", appendIndex " + appendIndex;
    }

    @Override
    public boolean isLast() {
        return true;
    }

    @Override
    public boolean isFirst() {
        return true;
    }

    @Override
    public boolean isRollback() {
        return false;
    }

    @Override
    public int commandCount() {
        return 0;
    }

    @Override
    public long appendIndex() {
        return appendIndex;
    }

    @Override
    public long chunkId() {
        return UNKNOWN_CHUNK_ID;
    }

    @Override
    public void setAppendIndex(long appendIndex) {}

    @Override
    public boolean accept(Visitor<StorageCommand, IOException> visitor) {
        return false;
    }

    @Override
    public Iterator<StorageCommand> iterator() {
        return Iterators.emptyResourceIterator();
    }

    @Override
    public KernelVersion kernelVersion() {
        return kernelVersion;
    }
}
