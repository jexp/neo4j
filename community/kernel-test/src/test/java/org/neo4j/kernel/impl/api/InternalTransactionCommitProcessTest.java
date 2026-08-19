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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.common.Subject.ANONYMOUS;
import static org.neo4j.internal.helpers.Exceptions.contains;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.KernelVersion.DEFAULT_BOOTSTRAP_VERSION;
import static org.neo4j.kernel.impl.api.CommandCommitListeners.NO_LISTENERS;
import static org.neo4j.storageengine.api.TransactionApplicationMode.INTERNAL;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;

import java.io.IOException;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectAssertions;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.io.pagecache.OutOfDiskSpaceException;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.txid.IdStoreTransactionIdGenerator;
import org.neo4j.kernel.impl.transaction.log.CompleteCommandBatch;
import org.neo4j.kernel.impl.transaction.log.FakeCommitment;
import org.neo4j.kernel.impl.transaction.log.LogAppendEvent;
import org.neo4j.kernel.impl.transaction.log.TestableTransactionAppender;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.log.TransactionCommitmentFactory;
import org.neo4j.kernel.impl.transaction.tracing.TransactionWriteEvent;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogAssertions;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.CommandBatch;
import org.neo4j.storageengine.api.Leases;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageEngineTransaction;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.LatestVersions;

class InternalTransactionCommitProcessTest {
    private final TransactionWriteEvent transactionWriteEvent = TransactionWriteEvent.NULL;

    @Test
    void shouldFailWithProperMessageOnAppendException() throws Exception {
        // GIVEN
        AssertableLogProvider logProvider = new AssertableLogProvider();

        TransactionAppender appender = mock(TransactionAppender.class);
        StorageEngine storageEngine = mock(StorageEngine.class);
        var commandCommitListeners = mock(CommandCommitListeners.class);

        IOException rootCause = new IOException("Mock exception");
        doThrow(new IOException(rootCause))
                .when(appender)
                .register(any(CompleteTransaction.class), any(LogAppendEvent.class));

        TransactionCommitProcess commitProcess = new InternalTransactionCommitProcess(
                appender, storageEngine, false, commandCommitListeners, () -> true, logProvider);

        // WHEN
        var mockedTransaction = mockedTransaction(mock(TransactionIdStore.class));
        var exceptionAssert = ErrorGqlStatusObjectAssertions.assertThatThrownBy(() -> commitProcess.commit(
                        mockedTransaction, transactionWriteEvent, INTERNAL, EmptyMemoryTracker.INSTANCE))
                .isInstanceOf(TransactionFailureException.class)
                .hasMessageContaining("Could not append transaction: ")
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_2DN06)
                .hasStatusDescription(
                        "error: invalid transaction termination - failed to append transaction. There was an error on appending the transaction. See logs for more information.");
        exceptionAssert.rootCause().isInstanceOf(IOException.class).hasMessageContaining("Mock exception");

        verify(commandCommitListeners).registerFailure(mockedTransaction, exceptionAssert.getActual());

        LogAssertions.assertThat(logProvider)
                .containsMessageWithException("Could not append transaction: ", exceptionAssert.getActual());
    }

    @Test
    void shouldFailWithProperMessageOnApplyException() throws Exception {
        // GIVEN
        TransactionAppender appender = mock(TransactionAppender.class);

        AssertableLogProvider logProvider = new AssertableLogProvider();
        StorageEngine storageEngine = mock(StorageEngine.class);
        var commandCommitListeners = mock(CommandCommitListeners.class);

        IOException rootCause = new IOException("Mock exception");
        doThrow(new IOException(rootCause))
                .when(storageEngine)
                .apply(
                        any(StorageEngineTransaction.class),
                        any(TransactionApplicationMode.class),
                        any(MemoryTracker.class));

        TransactionCommitProcess commitProcess = new InternalTransactionCommitProcess(
                appender, storageEngine, false, commandCommitListeners, () -> true, logProvider);

        // WHEN
        var mockedTransaction = mockedTransaction(mock(TransactionIdStore.class));
        var exceptionAssert = ErrorGqlStatusObjectAssertions.assertThatThrownBy(() -> commitProcess.commit(
                        mockedTransaction, transactionWriteEvent, INTERNAL, EmptyMemoryTracker.INSTANCE))
                .isInstanceOf(TransactionFailureException.class)
                .hasMessageContaining("Could not apply the transaction: ")
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_2DN05)
                .hasStatusDescription(
                        "error: invalid transaction termination - failed to apply transaction. There was an error on applying the transaction. See logs for more information.");
        exceptionAssert.rootCause().isInstanceOf(IOException.class).hasMessageContaining("Mock exception");

        verify(commandCommitListeners).registerFailure(mockedTransaction, exceptionAssert.getActual());

        LogAssertions.assertThat(logProvider)
                .containsMessageWithException("Could not apply the transaction: ", exceptionAssert.getActual());
    }

    @Test
    void shouldCloseTransactionRegardlessOfWhetherOrNotItAppliedCorrectly() throws Exception {
        // GIVEN
        TransactionIdStore transactionIdStore = mock(TransactionIdStore.class);
        TransactionAppender appender = new TestableTransactionAppender();
        long txId = 11;
        when(transactionIdStore.nextCommittingTransactionId()).thenReturn(txId);
        IOException rootCause = new IOException("Mock exception");
        StorageEngine storageEngine = mock(StorageEngine.class);
        doThrow(new IOException(rootCause))
                .when(storageEngine)
                .apply(any(CompleteTransaction.class), any(TransactionApplicationMode.class), any(MemoryTracker.class));
        var commandCommitListeners = mock(CommandCommitListeners.class);
        TransactionCommitProcess commitProcess = new InternalTransactionCommitProcess(
                appender, storageEngine, false, commandCommitListeners, () -> true, NullLogProvider.getInstance());
        CompleteTransaction transaction = mockedTransaction(transactionIdStore);

        // WHEN
        TransactionFailureException exception = assertThrows(
                TransactionFailureException.class,
                () -> commitProcess.commit(transaction, transactionWriteEvent, INTERNAL, EmptyMemoryTracker.INSTANCE));
        assertThat(exception.getMessage()).contains("Could not apply the transaction:");
        assertTrue(contains(exception, rootCause.getMessage(), rootCause.getClass()));
        verify(commandCommitListeners).registerFailure(transaction, exception);
        verify(commandCommitListeners, never()).registerSuccess(any());

        // THEN
        // we can't verify transactionCommitted since that's part of the TransactionAppender, which we have mocked
        verify(transactionIdStore)
                .transactionClosed(
                        eq(txId),
                        anyLong(),
                        any(KernelVersion.class),
                        anyLong(),
                        anyLong(),
                        anyInt(),
                        anyLong(),
                        anyLong());
    }

    @Test
    void commandBatchWithoutAppendIndexFailingToCommit() {
        long txId = 11;
        long appendIndex = txId + 7;

        var transactionIdStore = mock(TransactionIdStore.class);
        var appender = new TestableTransactionAppender();
        when(transactionIdStore.nextCommittingTransactionId()).thenReturn(txId);

        var storageEngine = mock(StorageEngine.class);
        var commitProcess = new InternalTransactionCommitProcess(
                appender, storageEngine, false, NO_LISTENERS, () -> true, NullLogProvider.getInstance());
        var batch = new CompleteCommandBatch(
                Collections.emptyList(),
                UNKNOWN_CONSENSUS_INDEX,
                -1,
                -1,
                -1,
                -1,
                Leases.NO_LEASES,
                LatestVersions.LATEST_KERNEL_VERSION,
                ANONYMOUS);
        var transactionToApply = new CompleteTransaction(
                batch,
                NULL_CONTEXT,
                StoreCursors.NULL,
                new FakeCommitment(txId, appendIndex, transactionIdStore, true),
                new IdStoreTransactionIdGenerator(transactionIdStore));

        assertThatThrownBy(() -> commitProcess.commit(
                        transactionToApply, transactionWriteEvent, INTERNAL, EmptyMemoryTracker.INSTANCE))
                .rootCause()
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Append index was not generated for the batch yet.");
    }

    @Test
    void shouldSuccessfullyCommitTransactionWithNoCommands() throws Exception {
        // GIVEN
        long txId = 11;
        long appendIndex = txId + 7;

        TransactionIdStore transactionIdStore = mock(TransactionIdStore.class);
        TransactionAppender appender = new TestableTransactionAppender();
        when(transactionIdStore.nextCommittingTransactionId()).thenReturn(txId);

        StorageEngine storageEngine = mock(StorageEngine.class);

        var commandCommitListeners = mock(CommandCommitListeners.class);
        TransactionCommitProcess commitProcess = new InternalTransactionCommitProcess(
                appender, storageEngine, false, commandCommitListeners, () -> true, NullLogProvider.getInstance());
        CompleteCommandBatch noCommandTx = new CompleteCommandBatch(
                Collections.emptyList(),
                UNKNOWN_CONSENSUS_INDEX,
                -1,
                -1,
                -1,
                -1,
                Leases.NO_LEASES,
                LatestVersions.LATEST_KERNEL_VERSION,
                ANONYMOUS);
        noCommandTx.setAppendIndex(appendIndex);

        // WHEN

        var transactionToApply = new CompleteTransaction(
                noCommandTx,
                NULL_CONTEXT,
                StoreCursors.NULL,
                new FakeCommitment(txId, appendIndex, transactionIdStore, true),
                new IdStoreTransactionIdGenerator(transactionIdStore));
        commitProcess.commit(transactionToApply, transactionWriteEvent, INTERNAL, EmptyMemoryTracker.INSTANCE);

        verify(transactionIdStore)
                .transactionCommitted(
                        txId,
                        appendIndex,
                        DEFAULT_BOOTSTRAP_VERSION,
                        FakeCommitment.CHECKSUM,
                        FakeCommitment.TIMESTAMP,
                        FakeCommitment.CONSENSUS_INDEX);
        verify(commandCommitListeners, never()).registerFailure(any(), any());
        verify(commandCommitListeners).registerSuccess(transactionToApply);
    }

    @Test
    void shouldFailWithOutOfDiskSpaceOnPreAllocationException() throws Exception {
        AssertableLogProvider logProvider = new AssertableLogProvider();

        TransactionAppender appender = mock(TransactionAppender.class);
        StorageEngine storageEngine = mock(StorageEngine.class);
        doThrow(new OutOfDiskSpaceException("test out of disk"))
                .when(storageEngine)
                .preAllocateStoreFilesForCommands(any(), any());
        var commandCommitListeners = mock(CommandCommitListeners.class);
        TransactionCommitProcess commitProcess = new InternalTransactionCommitProcess(
                appender, storageEngine, true, commandCommitListeners, () -> true, logProvider);

        var transaction = mockedTransaction(mock(TransactionIdStore.class));
        var exceptionAssert = ErrorGqlStatusObjectAssertions.assertThatThrownBy(() ->
                        commitProcess.commit(transaction, transactionWriteEvent, INTERNAL, EmptyMemoryTracker.INSTANCE))
                .isInstanceOf(TransactionFailureException.class)
                .hasMessageContaining("Could not preallocate disk space ")
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_51N59)
                .hasStatusDescription(
                        "error: system configuration or operation exception - internal resource exhaustion. The DBMS is unable to handle the request, please retry later or contact the system operator. More information is present in the logs.")
                .hasStatus(Status.General.UnknownError);
        // FIXME ODP this is not the status we should end up with in the end
        exceptionAssert
                .rootCause()
                .isInstanceOf(OutOfDiskSpaceException.class)
                .hasMessageContaining("test out of disk");

        verify(commandCommitListeners).registerFailure(transaction, exceptionAssert.getActual());

        LogAssertions.assertThat(logProvider)
                .containsMessageWithException("Could not preallocate disk space ", exceptionAssert.getActual());
    }

    @Test
    void shouldNotReportOutOfDiskSpaceOnGeneralIOException() throws Exception {
        AssertableLogProvider logProvider = new AssertableLogProvider();

        TransactionAppender appender = mock(TransactionAppender.class);
        StorageEngine storageEngine = mock(StorageEngine.class);
        doThrow(new IOException("IO exception other than out of disk"))
                .when(storageEngine)
                .preAllocateStoreFilesForCommands(any(), any());
        var commandCommitListeners = mock(CommandCommitListeners.class);
        TransactionCommitProcess commitProcess = new InternalTransactionCommitProcess(
                appender, storageEngine, true, commandCommitListeners, () -> true, logProvider);

        var transaction = mockedTransaction(mock(TransactionIdStore.class));
        var exceptionAssert = ErrorGqlStatusObjectAssertions.assertThatThrownBy(() ->
                        commitProcess.commit(transaction, transactionWriteEvent, INTERNAL, EmptyMemoryTracker.INSTANCE))
                .isInstanceOf(TransactionFailureException.class)
                .hasMessageContaining("Could not preallocate disk space ")
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_51N59)
                .hasStatusDescription(
                        "error: system configuration or operation exception - internal resource exhaustion. The DBMS is unable to handle the request, please retry later or contact the system operator. More information is present in the logs.")
                .hasStatus(Status.Transaction.TransactionCommitFailed);
        exceptionAssert
                .rootCause()
                .isInstanceOf(IOException.class)
                .hasMessageContaining("IO exception other than out of disk");

        verify(commandCommitListeners).registerFailure(transaction, exceptionAssert.getActual());

        LogAssertions.assertThat(logProvider)
                .containsMessageWithException("Could not preallocate disk space ", exceptionAssert.getActual());
    }

    @Test
    void shouldNotTryToPreallocateWhenDisabled() throws IOException, TransactionFailureException {
        TransactionAppender appender = mock(TransactionAppender.class);
        StorageEngine storageEngine = mock(StorageEngine.class);
        var commandCommitListeners = mock(CommandCommitListeners.class);
        TransactionCommitProcess commitProcess = new InternalTransactionCommitProcess(
                appender, storageEngine, false, commandCommitListeners, () -> true, NullLogProvider.getInstance());
        commitProcess.commit(
                mockedTransaction(mock(TransactionIdStore.class)),
                transactionWriteEvent,
                INTERNAL,
                EmptyMemoryTracker.INSTANCE);

        verify(storageEngine, never()).preAllocateStoreFilesForCommands(any(), any());
    }

    private CompleteTransaction mockedTransaction(TransactionIdStore transactionIdStore) {
        CommandBatch batch = mock(CommandBatch.class);
        when(batch.consensusIndex()).thenReturn(UNKNOWN_CONSENSUS_INDEX);
        when(batch.kernelVersion()).thenReturn(LatestVersions.LATEST_KERNEL_VERSION);
        var commitmentFactory = new TransactionCommitmentFactory(transactionIdStore);
        var transactionCommitment = commitmentFactory.newCommitment();
        return new CompleteTransaction(
                batch,
                NULL_CONTEXT,
                StoreCursors.NULL,
                transactionCommitment,
                new IdStoreTransactionIdGenerator(transactionIdStore));
    }
}
