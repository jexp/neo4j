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
package org.neo4j.wal.files.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.kernel.BinarySupportedKernelVersions;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.wal.AppendBatchInfo;
import org.neo4j.wal.LogFile;
import org.neo4j.wal.LogPosition;
import org.neo4j.wal.entry.UnsupportedLogVersionException;
import org.neo4j.wal.files.LogRangeInfo;

class DetachedLogTailAppendIndexProviderTest {
    private static final long LAST_KNOWN_APPEND_INDEX = 5;
    private static final LogPosition LAST_KNOWN_POSITION = new LogPosition(0, 128);

    private final Config config = Config.defaults();
    private final BinarySupportedKernelVersions binarySupportedKernelVersions =
            new BinarySupportedKernelVersions(config);
    private LogFile logFile;

    @BeforeEach
    void setUp() throws IOException {
        Path file = Path.of("neostore.transaction.db.0");
        logFile = mock(LogFile.class);
        when(logFile.versionExists(anyLong())).thenReturn(true);
        when(logFile.getLogRangeInfo()).thenReturn(new LogRangeInfo(0, file, 0, file));
        when(logFile.getReader(any(), any()))
                .thenThrow(UnsupportedLogVersionException.unsupported(
                        binarySupportedKernelVersions, KernelVersion.GLORIOUS_FUTURE.version()));
    }

    @Test
    void unsupportedLogVersionFallsBackToLastKnownAppendIndexByDefault() throws IOException {
        AppendBatchInfo lastBatch = provider(false).get();

        assertThat(lastBatch.appendIndex()).isEqualTo(LAST_KNOWN_APPEND_INDEX);
        assertThat(lastBatch.logPositionAfter()).isEqualTo(LAST_KNOWN_POSITION);
        verify(logFile).getReader(any(), any());
    }

    @Test
    void unsupportedLogVersionPropagatesUnwrappedWhenRequested() {
        assertThatThrownBy(() -> provider(true).get()).isInstanceOf(UnsupportedLogVersionException.class);
    }

    private DetachedLogTailAppendIndexProvider provider(boolean failOnUnsupportedLogVersion) {
        return new DetachedLogTailAppendIndexProvider(
                mock(CommandReaderFactory.class),
                binarySupportedKernelVersions,
                logFile,
                KernelVersion.getLatestVersion(config),
                LAST_KNOWN_APPEND_INDEX,
                LAST_KNOWN_POSITION,
                EmptyMemoryTracker.INSTANCE,
                LogPosition.UNSPECIFIED,
                failOnUnsupportedLogVersion);
    }
}
