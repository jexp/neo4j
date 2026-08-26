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
package org.neo4j.cli;

public interface CommandOptionDescriptions {

    class SplitArchiveOption {
        public static final String OPTION_NAME = "--split-archive-part-size";
        public static final String BACKUP_DESCRIPTION =
                "Splits the resulting backup artifact into multiple files of the specified size. "
                        + "The size can be specified in bytes or with a unit suffix (e.g. 5G, 100g, 1TiB). The minimum split size is 1GiB. "
                        + "If not specified the default value of 0 means the backup is not split and is written as a single file.";

        public static final String DUMP_DESCRIPTION =
                "Splits the resulting dump artifact into multiple files of the specified size. "
                        + "The size can be specified in bytes or with a unit suffix (e.g. 5G, 100g, 1TiB). The minimum split size is 1GiB. "
                        + "If not specified the default value of 0 means the dump is not split and is written as a single file.";
    }
}
