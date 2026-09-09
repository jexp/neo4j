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

import org.neo4j.internal.nativeimpl.NativeAccessProvider;

/// Holds the process-wide [RawVectorAdvisor], or the reason there is none.
///
/// There is one implementation, [NativeAccessRawVectorAdvisor], and it is unavailable wherever
/// `NativeAccessProvider` is -- which today means everything that is not Linux.
///
/// Absence is not an error: the caller falls back to [org.apache.lucene.store.IndexInput#prefetch].
/// It does change which configuration is running, which is why [#isAvailable] also gates the
/// raw-vector read advice in [LuceneKnnBinaryQuantizedVectorFormat#fieldsReader]. The two have to
/// move together: `MADV_RANDOM` on `.vec` gives up the kernel's fault-around, and the batched
/// prefetch is what pays that back. Advising without prefetching is therefore worse than doing
/// neither -- readahead is gone and nothing has queued the reads in its place.
final class RawVectorAdvisors {
    private static final RawVectorAdvisor NO_ADVICE = (slice, offset, length) -> false;

    private static final RawVectorAdvisor ADVISOR;

    /// Which provider is in use, or the reason there is none. Reachable for tests and diagnosis: an
    /// advisory call that silently does nothing is the failure this whole path exists to fix.
    private static final String PROVIDER;

    private RawVectorAdvisors() {}

    static {
        RawVectorAdvisor advisor = null;
        String reason = null;
        try {
            advisor = new NativeAccessRawVectorAdvisor(NativeAccessProvider.getNativeAccess());
        } catch (Throwable t) {
            reason = t.getClass().getSimpleName() + ": " + t.getMessage();
        }

        ADVISOR = advisor != null ? advisor : NO_ADVICE;
        PROVIDER = advisor != null ? advisor.getClass().getSimpleName() : "none (" + reason + ")";
    }

    /// Native access is unavailable on the platforms most tests run on, and everything gated on
    /// [#isAvailable] would otherwise go untested there -- including the raw vector read advice,
    /// which is only applied when advice can be paid for.
    private static volatile RawVectorAdvisor override;

    private static volatile boolean overridden;

    /// A null advisor stands for "unavailable", which is the case worth testing: it must switch the
    /// read advice off rather than leave it on unpaid for.
    static void overrideForTesting(RawVectorAdvisor advisor) {
        override = advisor;
        overridden = true;
    }

    static void clearOverrideForTesting() {
        override = null;
        overridden = false;
    }

    static RawVectorAdvisor advisor() {
        if (overridden) {
            RawVectorAdvisor overriding = override;
            return overriding != null ? overriding : NO_ADVICE;
        }
        return ADVISOR;
    }

    static boolean isAvailable() {
        return overridden ? override != null : ADVISOR != NO_ADVICE;
    }

    /// The provider in use, or the reason there is none.
    static String provider() {
        return PROVIDER;
    }

    /// Why advice stopped being issued after it started -- a kernel that rejected the call -- or null.
    static String unavailableReason() {
        return isAvailable() ? advisor().unavailableReason() : PROVIDER;
    }
}
