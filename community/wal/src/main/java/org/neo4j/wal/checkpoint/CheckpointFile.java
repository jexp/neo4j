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
package org.neo4j.wal.checkpoint;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.InternalLog;
import org.neo4j.wal.CheckpointInfo;
import org.neo4j.wal.LogTailMetadata;
import org.neo4j.wal.RotatableFile;
import org.neo4j.wal.VersionedFile;

/**
 * Access to underlying store checkpoints, that can be stored in multiple log files, separate log files etc.
 */
public interface CheckpointFile extends Lifecycle, VersionedFile, RotatableFile {

    /**
     * Last available checkpoint
     * @return last checkpoint
     */
    Optional<CheckpointInfo> findLatestCheckpoint() throws IOException;

    /**
     * Last available checkpoint
     * @param log custom log
     * @return last checkpoint
     */
    Optional<CheckpointInfo> findLatestCheckpoint(InternalLog log) throws IOException;

    /**
     * List of all reachable checkpoints from earliest to latest available
     * @return list of checkpoints, empty list if not reachable checkpoints are available
     */
    List<CheckpointInfo> reachableCheckpoints() throws IOException;

    /**
     * @return appender that aware how and where to append checkpoint record in particular implementation of the checkpoint file
     */
    CheckpointAppender getCheckpointAppender();

    /**
     * @return Information about log tail: records after checkpoint, missing logs etc
     */
    LogTailMetadata getTailMetadata();

    /**
     * @return checkpoint file that is currently used to store checkpoints into
     */
    Path getCurrentFile() throws IOException;
}
