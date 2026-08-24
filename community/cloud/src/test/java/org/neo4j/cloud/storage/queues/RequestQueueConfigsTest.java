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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.cloud.storage.StorageSettingsDeclaration.READ_IS_FOR_DESCRIPTION_FLAG;
import static org.neo4j.cloud.storage.StorageSettingsDeclaration.READ_IS_FOR_SAMPLING_FLAG;
import static org.neo4j.cloud.storage.queues.RequestQueueConfigs.DESCRIPTION_PULL_QUEUE_CHUNK_SIZE;
import static org.neo4j.cloud.storage.queues.RequestQueueConfigs.DESCRIPTION_PULL_QUEUE_SIZE;
import static org.neo4j.cloud.storage.queues.RequestQueueConfigs.SAMPLING_PULL_QUEUE_CHUNK_SIZE;
import static org.neo4j.cloud.storage.queues.RequestQueueConfigs.SAMPLING_PULL_QUEUE_SIZE;
import static org.neo4j.io.ByteUnit.mebiBytes;

import java.time.Duration;
import org.eclipse.collections.api.factory.Maps;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.cloud.storage.StoragePath;
import org.neo4j.cloud.storage.StorageSettingsDeclaration;
import org.neo4j.cloud.storage.StorageSystem;
import org.neo4j.cloud.storage.StorageSystemProvider;
import org.neo4j.cloud.storage.queues.RequestQueueConfigs.QueueConfig;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.Internal;
import org.neo4j.graphdb.config.Setting;

class RequestQueueConfigsTest {

    private static final int PUSH_SLOTS = 32;
    private static final long PUSH_CHUNKS = mebiBytes(4);

    private static final int PULL_SLOTS = 2;
    private static final long PULL_CHUNKS = mebiBytes(2);

    private static final Duration PUSH_TIMEOUT = Duration.ofSeconds(42);
    private static final Duration PULL_TIMEOUT = Duration.ofSeconds(69);

    private static final String NO_READ_FLAG = "cloud.storage.read.none";

    @Test
    void adaptPathForSampling() {
        final var path = mock(StoragePath.class);
        when(path.scheme()).thenReturn(TestSettings.SCHEME);
        when(path.copy()).thenReturn(path);

        StorageSettingsDeclaration.adaptPathForSampling(path);
        verify(path, times(1)).addMetadata(eq(READ_IS_FOR_SAMPLING_FLAG), eq(Boolean.TRUE));
    }

    @Test
    void adaptPathForDescription() {
        final var path = mock(StoragePath.class);
        when(path.scheme()).thenReturn(TestSettings.SCHEME);
        when(path.copy()).thenReturn(path);

        StorageSettingsDeclaration.adaptPathForDescription(path);
        verify(path, times(1)).addMetadata(eq(READ_IS_FOR_DESCRIPTION_FLAG), eq(Boolean.TRUE));
    }

    @Test
    void queueConfigConstructor() {
        assertThatThrownBy(() -> new QueueConfig(0, 1, PUSH_TIMEOUT)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new QueueConfig(1, 0, PULL_TIMEOUT)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new QueueConfig(1, 1, null)).isInstanceOf(NullPointerException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {NO_READ_FLAG, READ_IS_FOR_SAMPLING_FLAG, READ_IS_FOR_DESCRIPTION_FLAG})
    void create(String readFlag) {
        var provider = mock(StorageSystemProvider.class);
        when(provider.config())
                .thenReturn(Config.newBuilder()
                        .set(TestSettings.pull_queue_slot_size, PULL_SLOTS)
                        .set(TestSettings.pull_queue_chunk_size, PULL_CHUNKS)
                        .set(TestSettings.pull_queue_timeout, PULL_TIMEOUT)
                        .set(TestSettings.push_queue_slot_size, PUSH_SLOTS)
                        .set(TestSettings.push_queue_chunk_size, PUSH_CHUNKS)
                        .set(TestSettings.push_queue_timeout, PUSH_TIMEOUT)
                        .build());

        var fs = mock(StorageSystem.class);
        when(fs.provider()).thenReturn(provider);

        var path = mock(StoragePath.class);
        when(path.scheme()).thenReturn(TestSettings.SCHEME);
        when(path.getFileSystem()).thenReturn(fs);
        when(path.metadata()).thenReturn(Maps.immutable.of(readFlag, Boolean.TRUE));

        final var expectedSlots =
                switch (readFlag) {
                    case READ_IS_FOR_SAMPLING_FLAG -> SAMPLING_PULL_QUEUE_SIZE;
                    case READ_IS_FOR_DESCRIPTION_FLAG -> DESCRIPTION_PULL_QUEUE_SIZE;
                    default -> PULL_SLOTS;
                };
        final var expectedChunks =
                switch (readFlag) {
                    case READ_IS_FOR_SAMPLING_FLAG -> SAMPLING_PULL_QUEUE_CHUNK_SIZE;
                    case READ_IS_FOR_DESCRIPTION_FLAG -> DESCRIPTION_PULL_QUEUE_CHUNK_SIZE;
                    default -> (int) PULL_CHUNKS;
                };

        var queueConfigs = RequestQueueConfigs.create(path);

        assertThat(queueConfigs).isNotNull();
        assertThat(queueConfigs.pushConfig()).isNotNull().satisfies(config -> {
            assertThat(config.queueSize()).isEqualTo(PUSH_SLOTS);
            assertThat(config.chunkSize()).isEqualTo((int) PUSH_CHUNKS);
            assertThat(config.pollingTimeout()).isEqualTo(PUSH_TIMEOUT);
        });

        assertThat(queueConfigs.pullConfig()).isNotNull().satisfies(config -> {
            assertThat(config.queueSize()).isEqualTo(expectedSlots);
            assertThat(config.chunkSize()).isEqualTo(expectedChunks);
            assertThat(config.pollingTimeout()).isEqualTo(PULL_TIMEOUT);
        });
    }

    @ServiceProvider
    public static class TestSettings extends StorageSettingsDeclaration {

        public static final String SCHEME = "test";

        @Internal
        public static final Setting<Integer> push_queue_slot_size = pushQueueSlotSize(SCHEME);

        @Internal
        public static final Setting<Long> push_queue_chunk_size = pushQueueChunkSize(SCHEME);

        @Internal
        public static final Setting<Duration> push_queue_timeout = pushQueueTimeoutDuration(SCHEME);

        @Internal
        public static final Setting<Integer> pull_queue_slot_size = pullQueueSlotSize(SCHEME);

        @Internal
        public static final Setting<Long> pull_queue_chunk_size = pullQueueChunkSize(SCHEME);

        @Internal
        public static final Setting<Duration> pull_queue_timeout = pullQueueTimeoutDuration(SCHEME);
    }
}
