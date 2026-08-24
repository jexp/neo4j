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

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.storageengine.api.StorageEngineTransaction;

/**
 * Registers batches of commands, each containing groups of commands, as having been committed. For segregated log
 * implementations, this involves writing to a log that is guaranteed to be recoverable (i.e. consistently readable, in
 * the event of failure).
 */
public interface TransactionAppender extends Lifecycle {
    /**
     * Registers batches of commands as having been committed and made fully recoverable, and increments any transaction
     * counters. Callers must make sure that successfully appended command batches exiting this method are closed.
     * <p>
     * In segregated log implementations, this method also results in appending a batch of commands to the write-ahead
     * log. Failure happening inside such an implementation will cause a {@link DatabaseHealth#panic(Throwable) kernel
     * panic}.
     *
     *
     * @param batch command batch to register the commitment of.
     * @param logAppendEvent A trace event for the given log append operation.
     * @return last append index in this batch.
     * @throws IOException if there was a problem registering the command batch.
     */
    long register(StorageEngineTransaction batch, LogAppendEvent logAppendEvent)
            throws IOException, ExecutionException, InterruptedException;
}
