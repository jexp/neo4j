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
package org.neo4j.procedure.builtin;

import static java.util.Collections.emptyIterator;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.graphdb.TransactionTerminatedHelper;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.TokenSet;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.OtherThreadExecutor;

class SchemaCalculatorTerminationTest {
    private static final int ENTITY_COUNT = 100;
    private static final int PAUSE_AFTER_ENTITIES = 10;

    private final KernelTransaction ktx = mock(KernelTransaction.class);
    private final Read dataRead = mock(Read.class);
    private final TokenRead tokenRead = mock(TokenRead.class);
    private final CursorFactory cursors = mock(CursorFactory.class);

    private final CountDownLatch scanMayContinue = new CountDownLatch(1);
    private final AtomicBoolean terminated = new AtomicBoolean();
    private final AtomicInteger visitedEntities = new AtomicInteger();

    @BeforeEach
    void setUp() {
        when(ktx.dataRead()).thenReturn(dataRead);
        when(ktx.tokenRead()).thenReturn(tokenRead);
        when(ktx.cursors()).thenReturn(cursors);
        when(ktx.cursorContext()).thenReturn(CursorContext.NULL_CONTEXT);
        when(ktx.memoryTracker()).thenReturn(EmptyMemoryTracker.INSTANCE);
        doAnswer(invocation -> {
                    if (terminated.get()) {
                        throw TransactionTerminatedHelper.transactionTerminated(Status.Transaction.Terminated);
                    }
                    return null;
                })
                .when(ktx)
                .assertOpen();

        // The token reads deliberately ignore the termination, so that only the scan itself can fail the transaction
        when(tokenRead.propertyKeyGetAllTokens()).thenReturn(emptyIterator());
        when(tokenRead.labelsGetAllTokens()).thenReturn(emptyIterator());
        when(tokenRead.relationshipTypesGetAllTokens()).thenReturn(emptyIterator());

        when(cursors.allocatePropertyCursor(any(), any())).thenReturn(mock(PropertyCursor.class));
    }

    @Test
    void shouldStopScanningNodesWhenTransactionIsTerminated() throws Exception {
        var nodeCursor = mock(NodeCursor.class);
        when(nodeCursor.labels()).thenReturn(TokenSet.NONE);
        when(nodeCursor.next()).thenAnswer(invocation -> visitEntity());
        when(cursors.allocateNodeCursor(any(), any())).thenReturn(nodeCursor);

        assertTerminationStopsScan(SchemaCalculator::calculateTabularResultStreamForNodes);
    }

    @Test
    void shouldStopScanningRelationshipsWhenTransactionIsTerminated() throws Exception {
        var relationshipCursor = mock(RelationshipScanCursor.class);
        when(relationshipCursor.next()).thenAnswer(invocation -> visitEntity());
        when(cursors.allocateRelationshipScanCursor(any(), any())).thenReturn(relationshipCursor);

        assertTerminationStopsScan(SchemaCalculator::calculateTabularResultStreamForRels);
    }

    private void assertTerminationStopsScan(Function<SchemaCalculator, Stream<?>> scan) throws Exception {
        var calculator = new SchemaCalculator(ktx, false);
        try (var scanThread = new OtherThreadExecutor("scan")) {
            Future<?> scanning = scanThread.executeDontWait(() -> scan.apply(calculator));
            scanThread.waitUntilWaiting();

            terminated.set(true);
            scanMayContinue.countDown();

            assertThatThrownBy(() -> scanning.get(1, MINUTES))
                    .hasRootCauseInstanceOf(TransactionTerminatedException.class);
            assertThat(visitedEntities.get()).isLessThan(ENTITY_COUNT);
        }
    }

    private boolean visitEntity() throws InterruptedException {
        int visited = visitedEntities.incrementAndGet();
        if (visited == PAUSE_AFTER_ENTITIES) {
            scanMayContinue.await();
        }
        return visited <= ENTITY_COUNT;
    }
}
