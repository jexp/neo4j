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
package org.neo4j.kernel.impl.newapi;

import static java.lang.String.format;

import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.impl.iterator.ImmutableEmptyLongIterator;
import org.neo4j.collection.PrimitiveLongCollections;
import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.kernel.api.AccessModeProvider;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.storageengine.api.LongReference;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.StorageRelationshipTraversalCursor;

public class DefaultRelationshipTraversalCursor extends DefaultRelationshipCursor<DefaultRelationshipTraversalCursor>
        implements RelationshipTraversalCursor {
    private final StorageRelationshipTraversalCursor storeCursor;

    private long originNodeReference;
    private RelationshipSelection selection;
    private long neighbourNodeReference;

    protected DefaultRelationshipTraversalCursor(
            CursorPool<DefaultRelationshipTraversalCursor> pool,
            StorageRelationshipTraversalCursor storeCursor,
            InternalCursorFactory internalCursors,
            boolean applyAccessModeToTxState) {
        super(storeCursor, pool, applyAccessModeToTxState, internalCursors);
        this.storeCursor = storeCursor;
    }

    /**
     * Initializes this cursor to traverse over relationships, with a reference that was earlier retrieved from {@link NodeCursor#relationshipsReference()}.
     *
     * @param nodeReference reference to the origin node.
     * @param reference reference to the place to start traversing these relationships.
     * @param selection the relationship selector
     * @param read reference to {@link Read}.
     * @param txStateHolder transaction state holder
     * @param accessModeProvider access mode provider
     */
    void init(
            long nodeReference,
            long reference,
            RelationshipSelection selection,
            Read read,
            TxStateHolder txStateHolder,
            AccessModeProvider accessModeProvider) {
        init(read, txStateHolder, accessModeProvider);
        this.originNodeReference = nodeReference;
        this.selection = selection;
        this.neighbourNodeReference = LongReference.NULL;
        this.storeCursor.init(nodeReference, reference, selection);
    }

    /**
     * Initializes this cursor to traverse over relationships, directly from the {@link NodeCursor}.
     *
     * @param nodeCursor {@link NodeCursor} at the origin node.
     * @param selection the relationship selector
     * @param read reference to {@link Read}.
     * @param txStateHolder transaction state holder
     * @param accessModeProvider access mode provider
     */
    void init(
            DefaultNodeCursor nodeCursor,
            RelationshipSelection selection,
            Read read,
            TxStateHolder txStateHolder,
            AccessModeProvider accessModeProvider) {
        init(read, txStateHolder, accessModeProvider);
        this.originNodeReference = nodeCursor.nodeReference();
        this.selection = selection;
        this.neighbourNodeReference = LongReference.NULL;
        if (!nodeCursor.currentNodeIsAddedInChunk()) {
            nodeCursor.storeCursor.relationships(storeCursor, selection, nodeCursor.includeChangesFromThisTransaction);
        } else {
            storeCursor.reset();
        }
    }

    /**
     * Initializes this cursor to access a details of a relationship from the transaction state using the ID provided.
     *
     * @param addedRelationship the relationship to access
     * @param read reference to {@link Read}.
     * @param txStateHolder transaction state holder
     * @param accessModeProvider access mode provider
     */
    void init(long addedRelationship, Read read, TxStateHolder txStateHolder, AccessModeProvider accessModeProvider) {
        assert addedRelationship != LongReference.NULL;
        init(read, txStateHolder, accessModeProvider);
        this.originNodeReference = LongReference.NULL;
        this.neighbourNodeReference = LongReference.NULL;
        this.selection = null;
        storeCursor.reset();
        prepareChanges(PrimitiveLongCollections.single(addedRelationship), true);
    }

    void init(
            DefaultNodeCursor nodeCursor,
            RelationshipSelection selection,
            long neighbourNodeReference,
            Read read,
            TxStateHolder txStateHolder,
            AccessModeProvider accessModeProvider) {
        init(read, txStateHolder, accessModeProvider);
        this.originNodeReference = nodeCursor.nodeReference();
        this.selection = selection;
        this.neighbourNodeReference = neighbourNodeReference;
        if (!nodeCursor.currentNodeIsAddedInChunk()) {
            nodeCursor.storeCursor.relationshipsTo(storeCursor, selection, neighbourNodeReference);
        } else {
            storeCursor.reset();
        }
    }

    @Override
    protected void maybeTraceStoreHit() {}

    @Override
    public void otherNode(NodeCursor cursor) {
        read.singleNode(otherNodeReference(), cursor);
    }

    @Override
    public long otherNodeReference() {
        if (currentAddedInTx != LongReference.NULL) {
            // Here we compare the source/target nodes from tx-state to the origin node and decide the neighbour node
            // from it
            long originNodeReference = originNodeReference();
            if (txStateSourceNodeReference == originNodeReference) {
                return txStateTargetNodeReference;
            } else if (txStateTargetNodeReference == originNodeReference) {
                return txStateSourceNodeReference;
            } else {
                throw new IllegalStateException(format(
                        "Relationship[%d] which was added in tx has an origin node [%d] which is neither source [%d] nor target [%d]",
                        currentAddedInTx, originNodeReference, txStateSourceNodeReference, txStateTargetNodeReference));
            }
        }
        return storeCursor.neighbourNodeReference();
    }

    @Override
    public long originNodeReference() {
        return originNodeReference;
    }

    @Override
    protected boolean filterOutTxStateRelationship() {
        return neighbourNodeReference != LongReference.NULL && otherNodeReference() != neighbourNodeReference;
    }

    @Override
    public void setTracer(KernelReadTracer tracer) {
        super.setTracer(tracer);
        storeCursor.setTracer(tracer);
    }

    @Override
    public void removeTracer() {
        storeCursor.removeTracer();
        super.removeTracer();
    }

    @Override
    protected boolean allowedToTraverseEndNodes() {
        if (allowAllNodes) {
            return true;
        }
        // need to check only neighbour node because we came here from the source node
        var nodeCursor = getSecurityNodeCursor();
        read.singleNode(chooseNeighbourReferenceForSecurityCheck(), nodeCursor);
        return nodeCursor.next();
    }

    private long chooseNeighbourReferenceForSecurityCheck() {
        if (applyAccessModeToTxState
                && this.currentAddedInTx != LongReference.NULL
                && neighbourNodeReference != LongReference.NULL) {
            return neighbourNodeReference;
        }
        return storeCursor.neighbourNodeReference();
    }

    @Override
    public void closeInternal() {
        selection = null;
        super.closeInternal();
    }

    @Override
    protected LongIterator collectAddedTxStateSnapshot(TxStateHolder stateHolder) {
        if (selection != null) {
            return selection.addedRelationships(stateHolder.txState().getNodeState(originNodeReference));
        }
        return ImmutableEmptyLongIterator.INSTANCE;
    }

    @Override
    public String toString() {
        if (isClosed()) {
            return getClass().getSimpleName() + "[closed state]";
        }
        return getClass().getSimpleName() + "[id=" + storeCursor.entityReference() + ", " + storeCursor + "]";
    }
}
