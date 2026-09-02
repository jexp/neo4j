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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.database.DatabaseUpgradeTransactionHandler.DatabaseUpgradeListener.MultiVersionUpgradeGate;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.api.state.TxState;

class MultiVersionUpgradeLockTest {

    @Test
    void acquireUpgradePermit() {
        KernelTransactions transactions = mock(KernelTransactions.class);
        when(transactions.earliestTransactionSequenceNumber()).thenReturn(1L);
        KernelTransactionImplementation kernelTransaction = mock(KernelTransactionImplementation.class);
        when(kernelTransaction.getTransactionSequenceNumber()).thenReturn(1L);
        when(kernelTransaction.txState()).thenReturn(mock(TxState.class));

        DatabaseUpgradeTransactionHandler.DatabaseUpgradeListener.MultiVersionUpgradeGate upgradeLock =
                new DatabaseUpgradeTransactionHandler.DatabaseUpgradeListener.MultiVersionUpgradeGate(transactions);
        assertTrue(upgradeLock.upgradeGate(kernelTransaction));
    }

    @Test
    void multiChunkTransactionFailToAcquireUpgradePermit() {
        KernelTransactions transactions = mock(KernelTransactions.class);
        when(transactions.earliestTransactionSequenceNumber()).thenReturn(1L);
        KernelTransactionImplementation kernelTransaction = mock(KernelTransactionImplementation.class);
        when(kernelTransaction.getTransactionSequenceNumber()).thenReturn(1L);
        TxState txState = mock(TxState.class);
        when(txState.isMultiChunk()).thenReturn(true);
        when(kernelTransaction.txState()).thenReturn(txState);

        DatabaseUpgradeTransactionHandler.DatabaseUpgradeListener.MultiVersionUpgradeGate upgradeLock =
                new DatabaseUpgradeTransactionHandler.DatabaseUpgradeListener.MultiVersionUpgradeGate(transactions);
        assertFalse(upgradeLock.upgradeGate(kernelTransaction));
    }

    @Test
    void transactionWithHigherSequenceNumberBlocked() {
        KernelTransactions transactions = mock(KernelTransactions.class);
        when(transactions.earliestTransactionSequenceNumber()).thenReturn(5L);

        KernelTransactionImplementation kernelTransaction1 = mock(KernelTransactionImplementation.class);
        when(kernelTransaction1.getTransactionSequenceNumber()).thenReturn(5L);
        when(kernelTransaction1.txState()).thenReturn(mock(TxState.class));

        KernelTransactionImplementation kernelTransaction2 = mock(KernelTransactionImplementation.class);
        when(kernelTransaction2.getTransactionSequenceNumber()).thenReturn(10L);
        when(kernelTransaction2.txState()).thenReturn(mock(TxState.class));

        DatabaseUpgradeTransactionHandler.DatabaseUpgradeListener.MultiVersionUpgradeGate upgradeLock =
                new MultiVersionUpgradeGate(transactions);
        assertTrue(upgradeLock.upgradeGate(kernelTransaction1));

        try (var executor = Executors.newSingleThreadExecutor()) {
            try {
                Future<?> future = executor.submit(() -> upgradeLock.upgradeGate(kernelTransaction2));
                assertThrows(TimeoutException.class, () -> future.get(5, TimeUnit.SECONDS));
            } finally {
                when(transactions.earliestTransactionSequenceNumber()).thenReturn(10L);
                upgradeLock.release();
            }
        }
    }

    @Test
    void terminatedTransactionWaitingForUpgradeGateThrows() {
        KernelTransactions transactions = mock(KernelTransactions.class);
        when(transactions.earliestTransactionSequenceNumber()).thenReturn(5L);

        KernelTransactionImplementation gateOwner = mock(KernelTransactionImplementation.class);
        when(gateOwner.getTransactionSequenceNumber()).thenReturn(5L);
        when(gateOwner.txState()).thenReturn(mock(TxState.class));

        AtomicBoolean terminated = new AtomicBoolean();
        KernelTransactionImplementation waitingTransaction = mock(KernelTransactionImplementation.class);
        when(waitingTransaction.getTransactionSequenceNumber()).thenReturn(10L);
        when(waitingTransaction.isTerminated()).thenAnswer(invocation -> terminated.get());
        when(waitingTransaction.getReasonIfTerminated())
                .thenReturn(Optional.of(Status.Transaction.TransactionTimedOut));

        MultiVersionUpgradeGate upgradeLock = new MultiVersionUpgradeGate(transactions);
        assertTrue(upgradeLock.upgradeGate(gateOwner));

        try (var executor = Executors.newSingleThreadExecutor()) {
            Future<?> future = executor.submit(() -> upgradeLock.upgradeGate(waitingTransaction));
            assertThrows(TimeoutException.class, () -> future.get(1, TimeUnit.SECONDS));

            terminated.set(true);
            ExecutionException failure = assertThrows(ExecutionException.class, () -> future.get(1, TimeUnit.MINUTES));
            var terminatedException = assertInstanceOf(TransactionTerminatedException.class, failure.getCause());
            assertEquals(Status.Transaction.TransactionTimedOut, terminatedException.status());
        }

        // the gate is still owned by the transaction performing the upgrade
        KernelTransactionImplementation olderTransaction = mock(KernelTransactionImplementation.class);
        when(olderTransaction.getTransactionSequenceNumber()).thenReturn(3L);
        when(olderTransaction.txState()).thenReturn(mock(TxState.class));
        assertFalse(upgradeLock.upgradeGate(olderTransaction));
    }

    @Test
    void terminatedGateOwnerWaitingForOlderTransactionsReleasesGateAndThrows() {
        KernelTransactions transactions = mock(KernelTransactions.class);
        when(transactions.earliestTransactionSequenceNumber()).thenReturn(1L);

        AtomicBoolean terminated = new AtomicBoolean();
        KernelTransactionImplementation stuckTransaction = mock(KernelTransactionImplementation.class);
        when(stuckTransaction.getTransactionSequenceNumber()).thenReturn(5L);
        when(stuckTransaction.isTerminated()).thenAnswer(invocation -> terminated.get());
        when(stuckTransaction.getReasonIfTerminated()).thenReturn(Optional.empty());

        MultiVersionUpgradeGate upgradeLock = new MultiVersionUpgradeGate(transactions);

        try (var executor = Executors.newSingleThreadExecutor()) {
            Future<?> future = executor.submit(() -> upgradeLock.upgradeGate(stuckTransaction));
            assertThrows(TimeoutException.class, () -> future.get(1, TimeUnit.SECONDS));

            terminated.set(true);
            ExecutionException failure = assertThrows(ExecutionException.class, () -> future.get(1, TimeUnit.MINUTES));
            var terminatedException = assertInstanceOf(TransactionTerminatedException.class, failure.getCause());
            assertEquals(Status.Transaction.Terminated, terminatedException.status());
        }

        // the gate was released so an eligible transaction can acquire it
        KernelTransactionImplementation nextTransaction = mock(KernelTransactionImplementation.class);
        when(nextTransaction.getTransactionSequenceNumber()).thenReturn(1L);
        when(nextTransaction.txState()).thenReturn(mock(TxState.class));
        assertTrue(upgradeLock.upgradeGate(nextTransaction));
    }
}
