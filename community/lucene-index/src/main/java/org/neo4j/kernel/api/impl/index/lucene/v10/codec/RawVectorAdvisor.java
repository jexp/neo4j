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

import java.io.IOException;
import org.apache.lucene.store.IndexInput;

/// Asks the kernel to start reading a range of a memory mapped index file, so a batch of reads can be
/// in flight at once instead of one page fault at a time.
///
/// This exists because [IndexInput#prefetch], which would do the same job, throttles itself: it only
/// advises when a shared counter is zero or a power of two, and resets that counter only when the
/// advised pages turn out to be absent. Once part of the working set is resident the reset stops
/// happening, the counter ratchets, and the gate closes -- by which point it is advising almost
/// none of the batches it is handed, precisely when the working set no longer fits.
///
/// The implementation, [NativeAccessRawVectorAdvisor], issues `madvise(MADV_WILLNEED)` through
/// [org.neo4j.internal.nativeimpl.NativeAccess]. This interface talks about an [IndexInput] and a
/// range rather than a `MemorySegment` because that type is a preview API before Java 22 and cannot
/// be named in a module targeting the baseline; keeping it out of the signature is what lets
/// everything but the implementation stay fully typed.
public interface RawVectorAdvisor {
    /// Advises `[offset, offset + length)` of `slice`.
    ///
    /// @return whether the advice was issued. `false` means the caller should fall back to
    ///         [IndexInput#prefetch] -- the slice is not memory mapped, the range spans two mmap
    ///         chunks, or the syscall is unavailable on this platform.
    boolean willNeed(IndexInput slice, long offset, long length) throws IOException;

    /// Why advice is not being issued, or null when it is. Diagnosis only: an advisory call that
    /// silently does nothing is the exact failure this interface exists to fix, so the reason has to
    /// be reachable from somewhere.
    default String unavailableReason() {
        return null;
    }
}
