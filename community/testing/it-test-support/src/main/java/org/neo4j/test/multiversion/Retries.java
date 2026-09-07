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
package org.neo4j.test.multiversion;

import java.util.function.Consumer;
import org.neo4j.graphdb.TransientFailureException;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.storageengine.api.txstate.validation.TransactionConflictException;

public class Retries {

    public static boolean onTransient(Runnable runnable, Consumer<TransientFailureException> onRetry, int numAttempts) {
        while (true) {
            try {
                runnable.run();
                return true;
            } catch (TransientTransactionFailureException | TransactionConflictException exc) {
                if (onRetry != null) {
                    onRetry.accept(exc);
                }
                if (--numAttempts == 0) {
                    return false;
                }
            }
        }
    }

    public static boolean onTransient(Runnable runnable) {
        return onTransient(runnable, null, -1);
    }
}
