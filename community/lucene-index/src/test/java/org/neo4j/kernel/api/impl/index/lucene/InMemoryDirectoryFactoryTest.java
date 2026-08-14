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
package org.neo4j.kernel.api.impl.index.lucene;

import static org.apache.commons.lang3.RandomStringUtils.insecure;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.configuration.Config;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.impl.index.IndexWriterConfigBuilder;
import org.neo4j.kernel.api.impl.index.IndexWriterConfigMode;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class InMemoryDirectoryFactoryTest {
    private static final int DOCUMENTS = 1000;
    private static final long STREAM_BUFFER_SIZE = ByteUnit.kibiBytes(8);

    @Inject
    private TestDirectory testDirectory;

    private enum FileSystemType {
        DEFAULT,
        EPHEMERAL;

        FileSystemAbstraction create() {
            return this == EPHEMERAL ? new EphemeralFileSystemAbstraction() : new DefaultFileSystemAbstraction();
        }
    }

    static Stream<Arguments> luceneContextsAndFileSystems() {
        return Arrays.stream(LuceneContext.values())
                .flatMap(luceneContext -> Arrays.stream(FileSystemType.values())
                        .map(fileSystemType -> Arguments.of(luceneContext, fileSystemType)));
    }

    @ParameterizedTest
    @MethodSource("luceneContextsAndFileSystems")
    void shouldRestoreIndexLargerThanStreamBufferFromFileSystem(
            LuceneContext luceneContext, FileSystemType fileSystemType) throws Exception {
        try (FileSystemAbstraction fs = fileSystemType.create()) {
            Path indexFolder = testDirectory.homePath().resolve("index");

            try (DirectoryFactory directoryFactory =
                    luceneContext.directoryFactory().newInMemoryDirectoryFactory(fs)) {
                LuceneDirectory directory = directoryFactory.open(indexFolder);
                LuceneIndexWriterConfig writerConfig =
                        new IndexWriterConfigBuilder(IndexWriterConfigMode.TEXT, Config.defaults()).build();
                try (LuceneIndexWriter writer = directory.newWriter(writerConfig)) {
                    for (int i = 0; i < DOCUMENTS; i++) {
                        writer.addDocument(randomDocument(luceneContext));
                    }
                    writer.commit();
                }
            }

            assertThat(largestFileSize(fs, indexFolder))
                    .as("index has to exceed the stream buffer size to cover reads spanning multiple buffers")
                    .isGreaterThan(STREAM_BUFFER_SIZE);

            // A new factory starts with an empty cache and therefore has to reload the directory from the file system
            try (DirectoryFactory directoryFactory =
                    luceneContext.directoryFactory().newInMemoryDirectoryFactory(fs)) {
                LuceneDirectory directory = directoryFactory.open(indexFolder);

                assertThat(directory.checkIndexIsClean()).isTrue();
                try (LuceneDirectoryReader reader = directory.open()) {
                    assertThat(reader.newDirectSearcher().numDocs()).isEqualTo(DOCUMENTS);
                }
            }
        }
    }

    private static long largestFileSize(FileSystemAbstraction fs, Path folder) throws IOException {
        long largest = 0;
        for (Path file : fs.listFiles(folder)) {
            largest = Math.max(largest, fs.getFileSize(file));
        }
        return largest;
    }

    private static LuceneDocument randomDocument(LuceneContext luceneContext) {
        LuceneDocument document = luceneContext.documentsFactory().newDocument();
        document.addTextField("field", insecure().nextAlphanumeric(64), true);
        return document;
    }
}
