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
package org.neo4j.kernel.impl.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.batchimport.cache.idmapping.string.Workers;
import org.neo4j.internal.counts.GBPTreeCountsStore;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.transaction.EmptyBatchRepresentation;
import org.neo4j.kernel.impl.transaction.tracing.TransactionWriteEvent;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SkipOnSpd;
import org.neo4j.wal.TransactionCommitmentFactory;
import org.neo4j.wal.checkpoint.CheckPointer;
import org.neo4j.wal.checkpoint.SimpleTriggerInfo;

@ImpermanentDbmsExtension(configurationCallback = "configure")
class InternalTransactionCommitProcessIT {
    private static final int TOTAL_ACTIVE_THREADS = 6;

    @Inject
    private GraphDatabaseAPI db;

    @Inject
    private GBPTreeCountsStore countsStore;

    @Inject
    private CheckPointer checkPointer;

    @Inject
    private TransactionIdStore transactionIdStore;

    @ExtensionCallback
    static void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.setConfig(GraphDatabaseSettings.check_point_interval_time, Duration.ofMillis(10));
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void commitDuringContinuousCheckpointing() throws Exception {
        final AtomicBoolean done = new AtomicBoolean();
        Workers<Runnable> workers = new Workers<>(getClass().getSimpleName());
        try {
            for (int i = 0; i < TOTAL_ACTIVE_THREADS; i++) {
                workers.start(new Runnable() {
                    private final ThreadLocalRandom random = ThreadLocalRandom.current();

                    @Override
                    public void run() {
                        while (!done.get()) {
                            try (Transaction tx = db.beginTx()) {
                                tx.createNode();
                                tx.commit();
                            }
                            randomSleep();
                        }
                    }

                    private void randomSleep() {
                        try {
                            Thread.sleep(random.nextInt(50));
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
            }

            Thread.sleep(Duration.ofSeconds(2));
        } finally {
            done.set(true);
        }
        workers.awaitAndThrowOnError();

        assertThat(countsStore.txId())
                .as("Count store should be rotated once at least")
                .isGreaterThan(0L);

        long lastRotationTx = checkPointer.forceCheckPoint(new SimpleTriggerInfo("test"));
        assertEquals(
                transactionIdStore.getHighestGapFreeClosedTransactionId(),
                lastRotationTx,
                "NeoStore last closed transaction id should be equal last count store rotation transaction id.");
        assertEquals(
                transactionIdStore.getHighestGapFreeClosedTransactionId(),
                countsStore.txId(),
                "Last closed transaction should be last rotated tx in count store");
    }

    @Test
    @SkipOnSpd(reason = "Needs single instance and explicit tx hacking, shouldn't go though a cluster commit process")
    void emptyTransactionGetsHandled() throws Exception {
        TransactionCommitProcess transactionCommitProcess =
                db.getDependencyResolver().resolveDependency(TransactionCommitProcess.class);
        TransactionCommitmentFactory transactionCommitmentFactory =
                db.getDependencyResolver().resolveDependency(TransactionCommitmentFactory.class);

        AtomicLong txIdOfEmptyTx = new AtomicLong();

        CompleteTransaction completeTransaction = new CompleteTransaction(
                new EmptyBatchRepresentation(
                        LatestVersions.LATEST_KERNEL_VERSION,
                        transactionIdStore.getLastCommittedTransaction().appendIndex() + 1),
                CursorContext.NULL_CONTEXT,
                StoreCursors.NULL, // should be fine, don't want it to go to the stores anyway (except counts/degrees)
                transactionCommitmentFactory.newCommitment(),
                (id) -> {
                    long txId = transactionIdStore.nextCommittingTransactionId();
                    txIdOfEmptyTx.set(txId);
                    return txId;
                });
        transactionCommitProcess.commit(
                completeTransaction,
                TransactionWriteEvent.NULL,
                TransactionApplicationMode.INTERNAL,
                EmptyMemoryTracker.INSTANCE);

        long lastRotationTx = checkPointer.forceCheckPoint(new SimpleTriggerInfo("test"));

        // The empty tx should have been seen by both count store and the metadataprovider
        assertEquals(txIdOfEmptyTx.get(), lastRotationTx);
        assertEquals(transactionIdStore.getHighestGapFreeClosedTransactionId(), lastRotationTx);
        assertEquals(transactionIdStore.getHighestGapFreeClosedTransactionId(), countsStore.txId());
    }
}
