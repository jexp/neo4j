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
package org.neo4j.kernel.api.impl.index.lucene.v10.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Random;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.neo4j.internal.nativeimpl.AbsentNativeAccess;
import org.neo4j.internal.nativeimpl.NativeAccessProvider;

/// `NativeAccessProvider` only has a Linux implementation, so everything but the last two tests here
/// is skipped elsewhere. That is not a gap in coverage of the *decision*: [RawVectorAdvisors]
/// reporting no advisor is what switches the raw-vector read advice off, and `RawVectorsReadAdviceTest`
/// covers that on every platform through `overrideForTesting`.
class NativeAccessRawVectorAdvisorTest {
    private static final int FILE_BYTES = 1 << 20;

    @Test
    void reachTheSyscallOnThisPlatform() throws Throwable {
        assumeTrue(nativeAccessAvailable(), "no native access on this platform");
        assertThat(advisor().unavailableReason()).isNull();
    }

    /// The one thing no other test can establish: the syscall accepts the range and returns 0. There
    /// is no way to observe an advisory call from inside the JVM, so the return value is the evidence.
    @Test
    void adviseARealMapping(@TempDir Path dir) throws Throwable {
        assumeTrue(nativeAccessAvailable(), "no native access on this platform");
        assumeTrue(MMapDirectory.supportsMadvise(), "platform does not support madvise");
        NativeAccessRawVectorAdvisor advisor = advisor();

        withMappedFile(dir, input -> {
            assertThat(advisor.willNeed(input, 4096, 3072)).isTrue();
            assertThat(advisor.unavailableReason()).isNull();
        });
    }

    /// `madvise` rejects an unaligned address, so the advisor rounds the start down to a page. A
    /// vector is 3072 bytes at 768 dimensions, so almost every real range is unaligned.
    @Test
    void adviseAnUnalignedRange(@TempDir Path dir) throws Throwable {
        assumeTrue(nativeAccessAvailable(), "no native access on this platform");
        assumeTrue(MMapDirectory.supportsMadvise(), "platform does not support madvise");
        NativeAccessRawVectorAdvisor advisor = advisor();

        withMappedFile(dir, input -> {
            assertThat(advisor.willNeed(input, 3, 3072)).isTrue();
            assertThat(advisor.willNeed(input, 5 * 3072, 3072)).isTrue();
            assertThat(advisor.unavailableReason()).isNull();
        });
    }

    /// Rounding down at offset 0 must not step in front of the mapping.
    @Test
    void adviseFromTheStartOfTheFile(@TempDir Path dir) throws Throwable {
        assumeTrue(nativeAccessAvailable(), "no native access on this platform");
        assumeTrue(MMapDirectory.supportsMadvise(), "platform does not support madvise");
        NativeAccessRawVectorAdvisor advisor = advisor();

        withMappedFile(
                dir, input -> assertThat(advisor.willNeed(input, 0, 3072)).isTrue());
    }

    @Test
    void declineANonMappedSlice() throws Throwable {
        assumeTrue(nativeAccessAvailable(), "no native access on this platform");
        NativeAccessRawVectorAdvisor advisor = advisor();
        try (ByteBuffersDirectory directory = new ByteBuffersDirectory()) {
            try (IndexOutput out = directory.createOutput("vectors.bin", IOContext.DEFAULT)) {
                out.writeBytes(new byte[8192], 8192);
            }
            try (IndexInput input = directory.openInput("vectors.bin", IOContext.DEFAULT)) {
                assertThat(advisor.willNeed(input, 0, 3072)).isFalse();
            }
        }
        // Declining is not a failure -- the caller falls back to Lucene's own prefetch.
        assertThat(advisor.unavailableReason()).isNull();
    }

    @Test
    void declineAnEmptyRange(@TempDir Path dir) throws Throwable {
        assumeTrue(nativeAccessAvailable(), "no native access on this platform");
        NativeAccessRawVectorAdvisor advisor = advisor();
        withMappedFile(
                dir, input -> assertThat(advisor.willNeed(input, 4096, 0)).isFalse());
    }

    /// A platform without native access must not produce an advisor that silently does nothing: the
    /// read advice is gated on there being one, and it is only worth paying for when advice works.
    @Test
    void refuseToBuildWithoutNativeAccess() {
        assertThatThrownBy(() -> new NativeAccessRawVectorAdvisor(new AbsentNativeAccess()))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    /// Whether advice is available is decided by native access alone, so the two must agree --
    /// otherwise the raw-vector read advice, which is gated on [RawVectorAdvisors#isAvailable], is
    /// gated on something other than what actually issues the syscall. Where the syscall is reachable
    /// the provider also has to be the class that reaches it, rather than `NO_ADVICE` standing in for
    /// it silently.
    @Test
    void agreeWithNativeAccessOnAvailability() {
        assertThat(RawVectorAdvisors.isAvailable()).isEqualTo(nativeAccessAvailable());
        if (nativeAccessAvailable()) {
            assertThat(RawVectorAdvisors.provider()).isEqualTo("NativeAccessRawVectorAdvisor");
            assertThat(RawVectorAdvisors.unavailableReason()).isNull();
        }
    }

    private static boolean nativeAccessAvailable() {
        return NativeAccessProvider.getNativeAccess().isAvailable();
    }

    private static NativeAccessRawVectorAdvisor advisor() throws Throwable {
        return new NativeAccessRawVectorAdvisor(NativeAccessProvider.getNativeAccess());
    }

    private interface InputConsumer {
        void accept(IndexInput input) throws IOException;
    }

    private static void withMappedFile(Path dir, InputConsumer test) throws IOException {
        try (MMapDirectory directory = new MMapDirectory(dir)) {
            byte[] bytes = new byte[FILE_BYTES];
            new Random(7).nextBytes(bytes);
            try (IndexOutput out = directory.createOutput("vectors.bin", IOContext.DEFAULT)) {
                out.writeBytes(bytes, bytes.length);
            }
            try (IndexInput input = directory.openInput("vectors.bin", IOContext.DEFAULT)) {
                test.accept(input);
            }
        }
    }
}
