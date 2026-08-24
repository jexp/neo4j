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
package org.neo4j.cloud.storage.queues;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static org.neo4j.cloud.storage.StorageSettingsDeclaration.READ_IS_FOR_DESCRIPTION_FLAG;
import static org.neo4j.cloud.storage.StorageSettingsDeclaration.READ_IS_FOR_SAMPLING_FLAG;
import static org.neo4j.cloud.storage.StorageSettingsDeclaration.pullQueueChunkSize;
import static org.neo4j.cloud.storage.StorageSettingsDeclaration.pullQueuePollTimeout;
import static org.neo4j.cloud.storage.StorageSettingsDeclaration.pullQueueSlotSize;
import static org.neo4j.cloud.storage.StorageSettingsDeclaration.pushQueueChunkSize;
import static org.neo4j.cloud.storage.StorageSettingsDeclaration.pushQueuePollTimeout;
import static org.neo4j.cloud.storage.StorageSettingsDeclaration.pushQueueSlotSize;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.util.Preconditions.requirePositive;

import java.time.Duration;
import org.neo4j.cloud.storage.StoragePath;
import org.neo4j.cloud.storage.StorageSystemProvider;

/**
 * @param pushConfig the config to use when downloading content in a push fashion.
 * @param pullConfig the config to use when downloading content in a pull fashion.
 */
public record RequestQueueConfigs(QueueConfig pushConfig, QueueConfig pullConfig) {

    /**
     * This field and {@link RequestQueueConfigs#SAMPLING_PULL_QUEUE_CHUNK_SIZE} ensures that at most 2 chunks will be
     * loaded for a 4Mb sampling of CSV data for size estimations
     */
    public static final int SAMPLING_PULL_QUEUE_SIZE = 1;

    public static final int SAMPLING_PULL_QUEUE_CHUNK_SIZE = (int) (mebiBytes(2) + kibiBytes(100));

    /**
     * This field and {@link RequestQueueConfigs#DESCRIPTION_PULL_QUEUE_CHUNK_SIZE} ensure that an archive's description
     * can be read with a single request. A ZSTD compressed archive needs the 4 byte magic prefix plus the 131075 bytes
     * that {@code ZSTD_DStreamInSize} asks the underlying stream for before it will decode the first block; anything
     * smaller than that costs an extra round trip per description
     */
    public static final int DESCRIPTION_PULL_QUEUE_SIZE = 1;

    public static final int DESCRIPTION_PULL_QUEUE_CHUNK_SIZE = (int) kibiBytes(132);

    public RequestQueueConfigs {
        requireNonNull(pushConfig);
        requireNonNull(pullConfig);
    }

    public static RequestQueueConfigs create(StoragePath path) {
        // adapt the number of queue slots for when only the start of an object is being read (ex. getting headers,
        // estimating sizes in CSV imports, reading an archive's description, etc.) In this case, only a small amount
        // of the initial content is required so a large queue is unnecessary. In the 'normal' access style, a larger
        // queue would be beneficial to help saturate the network interface and speed up downloads
        return new RequestQueueConfigs(pushQueueConfig(path), pullQueueConfig(path));
    }

    private static boolean isFlagged(StoragePath path, String flag) {
        return path.metadata().getOrDefault(flag, Boolean.FALSE) == Boolean.TRUE;
    }

    private static QueueConfig pushQueueConfig(StoragePath path) {
        final var config = StorageSystemProvider.config(path);
        return new QueueConfig(
                config.get(pushQueueSlotSize(path)),
                toIntExact(config.get(pushQueueChunkSize(path))),
                config.get(pushQueuePollTimeout(path)));
    }

    private static QueueConfig pullQueueConfig(StoragePath path) {
        final var config = StorageSystemProvider.config(path);
        final var pollTimeout = config.get(pullQueuePollTimeout(path));
        if (isFlagged(path, READ_IS_FOR_DESCRIPTION_FLAG)) {
            return new QueueConfig(DESCRIPTION_PULL_QUEUE_SIZE, DESCRIPTION_PULL_QUEUE_CHUNK_SIZE, pollTimeout);
        } else if (isFlagged(path, READ_IS_FOR_SAMPLING_FLAG)) {
            return new QueueConfig(SAMPLING_PULL_QUEUE_SIZE, SAMPLING_PULL_QUEUE_CHUNK_SIZE, pollTimeout);
        } else {
            return new QueueConfig(
                    config.get(pullQueueSlotSize(path)), toIntExact(config.get(pullQueueChunkSize(path))), pollTimeout);
        }
    }

    /**
     * @param queueSize the size of the queue that maintains at most <code>queueSize</code> requests concurrently running
     * @param chunkSize the size of the data chunk to be downloaded in each request
     * @param pollingTimeout the timeout when polling for a chunk to be downloaded in the queue
     */
    public record QueueConfig(int queueSize, int chunkSize, Duration pollingTimeout) {
        public QueueConfig {
            requirePositive(queueSize);
            requirePositive(chunkSize);
            requireNonNull(pollingTimeout);
        }
    }
}
