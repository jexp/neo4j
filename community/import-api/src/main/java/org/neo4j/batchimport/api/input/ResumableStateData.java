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
package org.neo4j.batchimport.api.input;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public record ResumableStateData(byte stepOrdinal, long badCollectedEntriesCount, long problemHandlerPosition) {
    public byte[] asByteArray() throws IOException {
        ByteArrayOutputStream checkpoint = new ByteArrayOutputStream();
        DataOutputStream checkpointStream = new DataOutputStream(checkpoint);
        checkpointStream.writeByte(stepOrdinal);
        checkpointStream.writeLong(badCollectedEntriesCount);
        checkpointStream.writeLong(problemHandlerPosition);
        return checkpoint.toByteArray();
    }

    public static ResumableStateData fromInputStream(DataInputStream inputStream) throws IOException {
        byte stepOrdinal = inputStream.readByte();
        long collectorBadEntries = inputStream.readLong();
        long problemHandlerPosition = inputStream.readLong();
        return new ResumableStateData(stepOrdinal, collectorBadEntries, problemHandlerPosition);
    }

    @SuppressWarnings("UnusedReturnValue")
    public static class ResumableStateDataBuilder {
        private byte stepOrdinal;
        private long badCollectedEntriesCount;
        private long problemHandlerPosition;

        public ResumableStateDataBuilder setStepOrdinal(byte stepOrdinal) {
            this.stepOrdinal = stepOrdinal;
            return this;
        }

        public ResumableStateDataBuilder setBadCollectedEntriesCount(long badCollectedEntriesCount) {
            this.badCollectedEntriesCount = badCollectedEntriesCount;
            return this;
        }

        public ResumableStateDataBuilder setProblemHandlerPosition(long problemHandlerPosition) {
            this.problemHandlerPosition = problemHandlerPosition;
            return this;
        }

        public ResumableStateData build() {
            return new ResumableStateData(stepOrdinal, badCollectedEntriesCount, problemHandlerPosition);
        }
    }
}
