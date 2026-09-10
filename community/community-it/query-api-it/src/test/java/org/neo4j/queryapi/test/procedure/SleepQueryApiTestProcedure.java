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
package org.neo4j.queryapi.test.procedure;

import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureSignature;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.neo4j.collection.ResourceRawIterator;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.ResourceMonitor;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.IntegralValue;

/**
 * Represents a controllable "sleep" {@link CallableProcedure} for testing.
 * <p/>
 * The procedure can be called on Cypher by `queryApi.nightnight($timeout)`.
 * This procedure is not designed for being used with parallel call to itself.
 * So, It should NOT be used in parallel testing.
 */
public class SleepQueryApiTestProcedure {
    private final Controller controller;
    private final CallableProcedure.BasicProcedure callableProcedure;

    public SleepQueryApiTestProcedure() {
        this.controller = new Controller();
        this.callableProcedure = sleepProcedureFactory(controller);
    }

    /**
     *
     * @return The procedure instance to be installed in the Neo4j database.
     */
    public CallableProcedure callableProcedure() {
        return callableProcedure;
    }

    public Controller controller() {
        return controller;
    }

    private static CallableProcedure.BasicProcedure sleepProcedureFactory(Controller controller) {
        return new CallableProcedure.BasicProcedure(procedureSignature(new QualifiedName("queryAPI", "nightnight"))
                .in("data", Neo4jTypes.NTInteger)
                .out(ProcedureSignature.VOID)
                .build()) {
            @Override
            public ResourceRawIterator<AnyValue[], ProcedureException> apply(
                    org.neo4j.kernel.api.procedure.Context ctx, AnyValue[] input, ResourceMonitor resourceMonitor)
                    throws ProcedureException {
                try {
                    controller.notifyStart();
                    Thread.sleep(((IntegralValue) input[0]).longValue());
                } catch (InterruptedException e) {
                    throw ProcedureException.internalError(
                            this.getClass().getSimpleName(), "Interrupted", Status.General.UnknownError, e);
                }
                return ResourceRawIterator.empty();
            }
        };
    }

    /**
     * Used for receiving signals from the procedure.
     */
    public static class Controller {
        private final AtomicReference<CountDownLatch> startCountDownLatchRef;

        private Controller() {
            this.startCountDownLatchRef = new AtomicReference<>(new CountDownLatch(1));
        }

        private void notifyStart() {
            startCountDownLatchRef.get().countDown();
        }

        /**
         * Resets the awaiter
         * <p/>
         * This updates the object which the procedure will notify its start.
         */
        public void resetAwaiter() {
            startCountDownLatchRef.set(new CountDownLatch(1));
        }

        /**
         * Awaits to procedure notifies its start.
         *
         * @param timeout the maximum time to wait
         * @param timeUnit the time unit of the `timeout` argument
         * @return true if procedure notifies its start,
         *         false if the waiting time elapsed before the procedure notify its start.
         * @throws InterruptedException if the current thread is interrupted while waiting
         */
        public boolean awaitProcedureStarts(long timeout, TimeUnit timeUnit) throws InterruptedException {
            return startCountDownLatchRef.get().await(timeout, timeUnit);
        }
    }
}
