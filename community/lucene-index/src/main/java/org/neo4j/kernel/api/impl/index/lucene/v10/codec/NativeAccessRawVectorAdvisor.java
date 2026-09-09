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
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.neo4j.internal.nativeimpl.NativeAccess;
import org.neo4j.internal.nativeimpl.NativeCallResult;
import org.neo4j.internal.unsafe.UnsafeUtil;

/// `madvise(MADV_WILLNEED)` over a range of a memory mapped index file, issued through
/// [NativeAccess] -- the same JNA binding [NativeAccess#tryPopulateMemory] already uses for
/// `MADV_POPULATE_WRITE`.
///
/// The one thing the kernel needs that Neo4j's native layer cannot supply is the address: only
/// Lucene knows where it mapped the file, and it hands it over as a
/// `java.lang.foreign.MemorySegment` from [MemorySegmentAccessInput#segmentSliceOrNull]. That type is
/// a preview API before Java 22, so a module targeting the Java 21 baseline cannot *name* it -- but
/// two properties of javac's preview check make it reachable anyway:
///
///   - a method whose *return* type is preview may be called as long as the result is held as
///     [Object], which is how the segment gets here;
///   - reflection is not subject to the check at all, which is how [#address] reads it.
///
/// So the preview API is confined to one [MethodHandle] and the syscall is the one the product
/// already links. Replace [#address] with a plain `segment.address()` when the baseline reaches a
/// Java where the API is final.
final class NativeAccessRawVectorAdvisor implements RawVectorAdvisor {
    /// `(Object segment) -> long`, being `java.lang.foreign.MemorySegment#address`.
    private final MethodHandle address;

    private final NativeAccess nativeAccess;

    private final int pageSize;

    /// Set once, never cleared: a call the kernel keeps rejecting should not be repeated per
    /// candidate, and a prefetch that silently does nothing is the failure this class exists to fix.
    private volatile String disabled;

    /// Throws if native access is unavailable -- which is every non-Linux platform, since
    /// `NativeAccessProvider` only has a Linux implementation -- or if the preview API cannot be
    /// reached. [RawVectorAdvisors] treats either as "no advice available".
    NativeAccessRawVectorAdvisor(NativeAccess nativeAccess) throws Throwable {
        if (!nativeAccess.isAvailable()) {
            throw new UnsupportedOperationException(nativeAccess.describe());
        }
        this.nativeAccess = nativeAccess;
        this.address = MethodHandles.publicLookup()
                .findVirtual(
                        Class.forName("java.lang.foreign.MemorySegment"), "address", MethodType.methodType(long.class))
                .asType(MethodType.methodType(long.class, Object.class));
        this.pageSize = UnsafeUtil.pageSize();
    }

    @Override
    public boolean willNeed(IndexInput slice, long offset, long length) throws IOException {
        if (disabled != null || length <= 0 || !(slice instanceof MemorySegmentAccessInput mapped)) {
            return false;
        }
        // A `java.lang.foreign.MemorySegment`, held as Object because its type cannot be named here.
        // Null when the range crosses two mmap chunks, which no single madvise call can cover.
        Object segment = mapped.segmentSliceOrNull(offset, length);
        if (segment == null) {
            return false;
        }

        long start;
        try {
            start = (long) address.invokeExact(segment);
        } catch (Throwable t) {
            disabled = t.getClass().getSimpleName() + ": " + t.getMessage();
            return false;
        }

        // madvise rejects an unaligned address, so round the start down to a page and lengthen the
        // range to match. That cannot walk off the front of the mapping: an mmap base is page
        // aligned, so the largest page boundary at or below any address inside it is still inside it.
        long misaligned = Math.floorMod(start, (long) pageSize);

        NativeCallResult result = nativeAccess.tryAdviseWillNeedMemory(start - misaligned, length + misaligned);
        if (result.isError()) {
            disabled = result.toString();
            return false;
        }
        return true;
    }

    @Override
    public String unavailableReason() {
        return disabled;
    }
}
