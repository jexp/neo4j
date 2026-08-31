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
package org.neo4j.io.pagecache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import org.junit.jupiter.api.Test;

class StubPageCursorTest {
    private static final int PAGE_SIZE = 128;

    @Test
    void getBytesMustTolerateOffsetAlreadyPastPageEnd() {
        try (StubPageCursor cursor = new StubPageCursor(0, PAGE_SIZE)) {
            cursor.setOffset(PAGE_SIZE - Long.BYTES);
            cursor.getLong();
            cursor.getLong();
            assertThatCode(() -> cursor.getBytes(new byte[1])).doesNotThrowAnyException();
            assertThat(cursor.checkAndClearBoundsFlag()).isTrue();
        }
    }

    @Test
    void putBytesMustTolerateOffsetAlreadyPastPageEnd() {
        try (StubPageCursor cursor = new StubPageCursor(0, PAGE_SIZE)) {
            cursor.setOffset(PAGE_SIZE - Long.BYTES);
            cursor.putLong(0L);
            cursor.putLong(0L);
            assertThatCode(() -> cursor.putBytes(new byte[1])).doesNotThrowAnyException();
            assertThat(cursor.checkAndClearBoundsFlag()).isTrue();
        }
    }
}
