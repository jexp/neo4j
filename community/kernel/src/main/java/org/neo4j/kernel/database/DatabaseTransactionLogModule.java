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
package org.neo4j.kernel.database;

import org.neo4j.wal.LogicalTransactionStore;
import org.neo4j.wal.TransactionAppender;
import org.neo4j.wal.checkpoint.CheckPointer;

class DatabaseTransactionLogModule {
    private final CheckPointer checkPointer;
    private final TransactionAppender appender;
    private final LogicalTransactionStore logicalTransactionStore;

    DatabaseTransactionLogModule(
            CheckPointer checkPointer, TransactionAppender appender, LogicalTransactionStore logicalTransactionStore) {
        this.checkPointer = checkPointer;
        this.appender = appender;
        this.logicalTransactionStore = logicalTransactionStore;
    }

    public LogicalTransactionStore getLogicalTransactionStore() {
        return logicalTransactionStore;
    }

    CheckPointer checkPointer() {
        return checkPointer;
    }

    TransactionAppender transactionAppender() {
        return appender;
    }
}
