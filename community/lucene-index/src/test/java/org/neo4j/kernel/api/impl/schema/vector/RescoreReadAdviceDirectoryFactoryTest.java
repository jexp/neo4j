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
package org.neo4j.kernel.api.impl.schema.vector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.kernel.api.impl.index.lucene.LuceneSettings.vector_rescore_read_advice;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.function.BiFunction;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NRTCachingDirectory;
import org.apache.lucene.store.ReadAdvice;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.neo4j.configuration.Config;
import org.neo4j.kernel.api.impl.index.lucene.LuceneContext;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDirectory;
import org.neo4j.kernel.api.impl.index.lucene.v10.Lucene10Directory;
import org.neo4j.kernel.api.impl.index.lucene.v10.codec.RawVectorsReadAdvice;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;

class RescoreReadAdviceDirectoryFactoryTest {
    /// The default: the raw vector read advice is installed, so a marked `.vec` context resolves
    /// to `RANDOM` while everything else keeps the mmap default.
    @Test
    void installTheReadAdviceByDefault(@TempDir Path dir) throws IOException {
        try (RecordingMMapDirectory mmap = new RecordingMMapDirectory(dir)) {
            LuceneDirectory directory = factoryWith(Config.defaults(), mmap).open(dir);

            assertThat(directory).isNotNull();
            assertThat(mmap.installed).isSameAs(RawVectorsReadAdvice.READ_ADVICE);
        }
    }

    /// The setting off must leave nothing installed: [MMapDirectory] then keeps every file --
    /// `.vec` included -- on its default advice, the kernel's fault-around readahead, which is the
    /// measured baseline. Installing nothing is also what keeps Lucene's own `RANDOM` access hint
    /// inert for the graph and the codes, as it is for every unmarked file.
    @Test
    void installNothingWhenTheSettingIsOff(@TempDir Path dir) throws IOException {
        Config config =
                Config.newBuilder().set(vector_rescore_read_advice, false).build();

        try (RecordingMMapDirectory mmap = new RecordingMMapDirectory(dir)) {
            LuceneDirectory directory = factoryWith(config, mmap).open(dir);

            assertThat(directory).isNotNull();
            assertThat(mmap.installed)
                    .as("nothing must be installed on the mmap directory")
                    .isNull();
        }
    }

    /// Byte-buffer directories, which the in-memory factory hands out, have no advice to give.
    @Test
    void leaveNonMmapDirectoriesAlone() throws IOException {
        DirectoryFactory delegate = directoryFactory(new Lucene10Directory(new ByteBuffersDirectory()));
        RescoreReadAdviceDirectoryFactory factory = new RescoreReadAdviceDirectoryFactory(delegate, Config.defaults());

        try (LuceneDirectory directory = factory.open(null)) {
            assertThat(directory).isNotNull();
        }
    }

    private static DirectoryFactory factoryWith(Config config, RecordingMMapDirectory mmap) {
        return new RescoreReadAdviceDirectoryFactory(
                directoryFactory(new Lucene10Directory(new NRTCachingDirectory(mmap, 1, 1))), config);
    }

    private static DirectoryFactory directoryFactory(LuceneDirectory directory) {
        return new DirectoryFactory() {
            @Override
            public LuceneDirectory open(Path dir) {
                return directory;
            }

            @Override
            public LuceneContext getContext() {
                return LuceneContext.LUCENE_10;
            }

            @Override
            public void close() {}
        };
    }

    private static final class RecordingMMapDirectory extends MMapDirectory {
        private BiFunction<String, IOContext, Optional<ReadAdvice>> installed;

        RecordingMMapDirectory(Path dir) throws IOException {
            super(dir);
        }

        @Override
        public void setReadAdvice(BiFunction<String, IOContext, Optional<ReadAdvice>> readAdvice) {
            this.installed = readAdvice;
            super.setReadAdvice(readAdvice);
        }
    }
}
