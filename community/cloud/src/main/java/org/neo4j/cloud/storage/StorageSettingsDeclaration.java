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
package org.neo4j.cloud.storage;

import static org.neo4j.configuration.SettingConstraints.min;
import static org.neo4j.configuration.SettingConstraints.range;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BYTES;
import static org.neo4j.configuration.SettingValueParsers.DURATION;
import static org.neo4j.configuration.SettingValueParsers.INT;
import static org.neo4j.io.ByteUnit.gibiBytes;
import static org.neo4j.io.ByteUnit.mebiBytes;

import java.time.Duration;
import java.util.Objects;
import org.neo4j.cloud.storage.queues.PullQueue;
import org.neo4j.cloud.storage.queues.PushQueue;
import org.neo4j.configuration.SettingBuilder;
import org.neo4j.configuration.SettingConstraint;
import org.neo4j.configuration.SettingValueParser;
import org.neo4j.configuration.SettingsDeclaration;
import org.neo4j.graphdb.config.Setting;

public abstract class StorageSettingsDeclaration implements SettingsDeclaration {

    public static final long CHUNK_SIZE = mebiBytes(8);

    public static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofMinutes(5);

    public static final int MINIMUM_INFLIGHT_WRITE_REQUESTS = 3;
    public static final int MAXIMUM_INFLIGHT_WRITE_REQUESTS = 100;

    public static final String READ_IS_FOR_SAMPLING_FLAG = "cloud.storage.read.sampling";

    public static final String READ_IS_FOR_DESCRIPTION_FLAG = "cloud.storage.read.description";

    private static final SettingConstraint<Long> CHUNK_RANGE = range(mebiBytes(1), gibiBytes(1));

    static final String INTERNAL_CONFIG_PREFIX = "internal.dbms.cloud.storage";

    /**
     * Adapts a {@link StoragePath} so that any future partial reads of its data (i.e. for sampling) can use an optimised
     * @param path the path to adapt
     * @return a new {@link StoragePath} that has the {@link #READ_IS_FOR_SAMPLING_FLAG} set to <code>true</code>
     */
    public static StoragePath adaptPathForSampling(StoragePath path) {
        return path.copy().addMetadata(READ_IS_FOR_SAMPLING_FLAG, Boolean.TRUE);
    }

    /**
     * Adapts a {@link StoragePath} so that reads of an archive's description can use an optimised, much smaller,
     * request size than the {@link #adaptPathForSampling(StoragePath) sampling} one
     * @param path the path to adapt
     * @return a new {@link StoragePath} that has the {@link #READ_IS_FOR_DESCRIPTION_FLAG} set to <code>true</code>
     */
    public static StoragePath adaptPathForDescription(StoragePath path) {
        return path.copy().addMetadata(READ_IS_FOR_DESCRIPTION_FLAG, Boolean.TRUE);
    }

    /**
     * @param path the path to use for determining the setting
     * @return the appropriate push queue slot size setting (based on the {@link StoragePath} scheme
     */
    public static Setting<Integer> pushQueueSlotSize(StoragePath path) {
        return pushQueueSlotSize(scheme(path));
    }

    /**
     * @param path the path to use for determining the setting
     * @return the appropriate push queue chunk size setting (based on the {@link StoragePath} scheme
     */
    public static Setting<Long> pushQueueChunkSize(StoragePath path) {
        return pushQueueChunkSize(scheme(path));
    }

    /**
     * @param path the path to use for determining the setting
     * @return the appropriate push queue poll timeout (based on the {@link StoragePath} scheme
     */
    public static Setting<Duration> pushQueuePollTimeout(StoragePath path) {
        return pushQueueTimeoutDuration(scheme(path));
    }

    /**
     * @param path the path to use for determining the setting
     * @return the appropriate pull queue slot size setting (based on the {@link StoragePath} scheme
     */
    public static Setting<Integer> pullQueueSlotSize(StoragePath path) {
        return pullQueueSlotSize(scheme(path));
    }

    /**
     * @param path the path to use for determining the setting
     * @return the appropriate pull queue chunk size setting (based on the {@link StoragePath} scheme
     */
    public static Setting<Long> pullQueueChunkSize(StoragePath path) {
        return pullQueueChunkSize(scheme(path));
    }

    /**
     * @param path the path to use for determining the setting
     * @return the appropriate pull queue poll timeout (based on the {@link StoragePath} scheme
     */
    public static Setting<Duration> pullQueuePollTimeout(StoragePath path) {
        return pullQueueTimeoutDuration(scheme(path));
    }

    protected static Setting<Integer> pushQueueSlotSize(String scheme) {
        return queueOption(scheme, "push", "slot_size", INT, PushQueue.QUEUE_SIZE)
                .addConstraint(min(16))
                .build();
    }

    protected static Setting<Long> pushQueueChunkSize(String scheme) {
        return queueOption(scheme, "push", "chunk_size", BYTES, CHUNK_SIZE)
                .addConstraint(CHUNK_RANGE)
                .build();
    }

    protected static Setting<Duration> pushQueueTimeoutDuration(String scheme) {
        return queueOption(scheme, "push", "poll_timeout", DURATION, DEFAULT_POLL_TIMEOUT)
                .build();
    }

    protected static Setting<Integer> pullQueueSlotSize(String scheme) {
        return queueOption(scheme, "pull", "slot_size", INT, defaultPullQueueSize())
                .addConstraint(min(1))
                .build();
    }

    protected static Setting<Long> pullQueueChunkSize(String scheme) {
        return queueOption(scheme, "pull", "chunk_size", BYTES, CHUNK_SIZE)
                .addConstraint(CHUNK_RANGE)
                .build();
    }

    protected static Setting<Duration> pullQueueTimeoutDuration(String scheme) {
        return queueOption(scheme, "pull", "poll_timeout", DURATION, DEFAULT_POLL_TIMEOUT)
                .build();
    }

    protected static <S> SettingBuilder<S> publicOption(
            String scheme, String optionName, SettingValueParser<S> parser, S defaultValue) {
        return newBuilder("dbms.integrations.cloud_storage.%s.%s".formatted(scheme, optionName), parser, defaultValue);
    }

    protected static <S> SettingBuilder<S> internalOption(
            String scheme, String optionName, SettingValueParser<S> parser, S defaultValue) {
        return newBuilder("%s.%s.%s".formatted(INTERNAL_CONFIG_PREFIX, scheme, optionName), parser, defaultValue);
    }

    protected static int maxInflightRequestsBasedOffCores() {
        return Math.clamp(factorScaledByCores(3), MINIMUM_INFLIGHT_WRITE_REQUESTS, MAXIMUM_INFLIGHT_WRITE_REQUESTS);
    }

    private static <S> SettingBuilder<S> queueOption(
            String scheme, String queueType, String optionType, SettingValueParser<S> parser, S defaultValue) {
        return internalOption(scheme, "%s_queue_%s".formatted(queueType, optionType), parser, defaultValue);
    }

    private static String scheme(StoragePath path) {
        return Objects.requireNonNull(path).scheme();
    }

    private static int defaultPullQueueSize() {
        // when running on 96 core machine, testing found that a good queue size was 32
        return Math.max(PullQueue.QUEUE_SIZE, factorScaledByCores(1));
    }

    private static int factorScaledByCores(int shift) {
        return Integer.highestOneBit(Math.max(Runtime.getRuntime().availableProcessors() / 6, 1)) << shift;
    }
}
