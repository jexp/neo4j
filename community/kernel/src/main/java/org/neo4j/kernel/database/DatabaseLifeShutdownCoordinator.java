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

import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.wal.checkpoint.CheckpointerLifecycle;

public class DatabaseLifeShutdownCoordinator extends LifecycleAdapter {
    private final LifeSupport lifeSupport = new LifeSupport();

    DatabaseLifeShutdownCoordinator(
            DatabaseAvailabilityGuard databaseAvailabilityGuard,
            KernelTransactions kernelTransactions,
            CheckpointerLifecycle checkpointerLifecycle,
            MultiVersionDatabaseRollbackService multiVersionDatabaseRollbackService) {
        lifeSupport.add(checkpointerLifecycle);
        lifeSupport.add(kernelTransactions);
        lifeSupport.add(multiVersionDatabaseRollbackService);
        lifeSupport.add(databaseAvailabilityGuard);
    }

    @Override
    public void init() throws Exception {
        lifeSupport.init();
    }

    @Override
    public void start() throws Exception {
        lifeSupport.start();
    }

    @Override
    public void stop() throws Exception {
        // Steps on stop:
        // - make database not available
        // - block new transactions, wait for completion and do some termination of anything that left
        // After this moment components can perform stop actions of their desire but no new transactions are possible
        lifeSupport.stop();
    }

    @Override
    public void shutdown() throws Exception {
        // Steps on shutdown:
        // - database guard will say that its on shutdown
        // - shutdown mvcc transaction rollbacker
        // - kernel transactions will close all the resources
        // - we do checkpoint since no more writes should be performed into the database
        lifeSupport.shutdown();
    }
}
