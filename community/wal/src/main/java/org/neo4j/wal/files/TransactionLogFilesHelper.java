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
package org.neo4j.wal.files;

import static java.util.regex.Pattern.quote;

import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.function.Predicate;
import org.neo4j.io.fs.filename.SequentialFileNameHelper;

public final class TransactionLogFilesHelper {
    public static final String DEFAULT_NAME = "neostore.transaction.db";
    public static final String CHECKPOINT_FILE_PREFIX = "checkpoint";
    public static final DirectoryStream.Filter<Path> DEFAULT_FILENAME_FILTER =
            new SequentialFileNameHelper.SequentialFileNameFilter(quote(DEFAULT_NAME), quote(CHECKPOINT_FILE_PREFIX));
    public static final Predicate<String> DEFAULT_FILENAME_PREDICATE =
            file -> file.startsWith(DEFAULT_NAME) || file.startsWith(CHECKPOINT_FILE_PREFIX);

    private TransactionLogFilesHelper() {}

    public static SequentialFileNameHelper forTransactions(Path dir) {
        return new SequentialFileNameHelper(dir, DEFAULT_NAME);
    }

    public static SequentialFileNameHelper forCheckpoints(Path dir) {
        return new SequentialFileNameHelper(dir, CHECKPOINT_FILE_PREFIX);
    }
}
