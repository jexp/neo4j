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
package org.neo4j.io.pagecache.impl.muninn;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.io.ByteUnit.mebiBytes;

import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class LargePageMetadataIT {
    @Test
    void veryLargePageMetadataMustNotOverflowAndBeAccessible() {
        // We need roughly 2 GiBs of memory for the meta-data here, which is why this is an IT and not a Test.
        // use small page size to avoid allocating huge buffer for the pages
        int pageSize = 1;
        int pages = Math.toIntExact((Integer.MAX_VALUE + mebiBytes(64)) / PageMetadata.META_DATA_BYTES_PER_PAGE);
        try (var pageMetadata = new PageMetadata(pages, pageSize)) {
            assertThat(pageMetadata.getPageCount()).isEqualTo(pages);

            // Spot-check the accessibility in the bulk of the pages.
            IntStream.range(0, pages / 32)
                    .parallel()
                    .forEach(id -> verifyPageMetaDataIsAccessible(pageMetadata, id * 32));

            // Thoroughly check the accessibility around the tail end of the page list.
            IntStream.range(pages - 2000, pages)
                    .parallel()
                    .forEach(id -> verifyPageMetaDataIsAccessible(pageMetadata, id));
        }
    }

    private static void verifyPageMetaDataIsAccessible(PageMetadata pageMetadata, int id) {
        long ref = pageMetadata.deref(id);
        PageMetadata.incrementUsage(ref);
        PageMetadata.incrementUsage(ref);
        assertFalse(PageMetadata.decrementUsage(ref));
        assertTrue(PageMetadata.decrementUsage(ref));
        assertEquals(id, pageMetadata.toId(ref));
    }
}
