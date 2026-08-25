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

import static org.neo4j.storageengine.api.LongReference.NULL;
import static org.neo4j.storageengine.api.LongReference.NULL_REFERENCE;

import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.eclipse.collections.impl.iterator.ImmutableEmptyLongIterator;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;
import org.neo4j.collection.PrimitiveLongCollections;
import org.neo4j.collection.diffset.IntDiffSets;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.TokenSet;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.kernel.api.AccessModeProvider;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.storageengine.api.AllNodeScan;
import org.neo4j.storageengine.api.Degrees;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.Reference;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.txstate.NodeState;
import org.neo4j.storageengine.util.EagerDegrees;
import org.neo4j.storageengine.util.SingleDegree;

public class DefaultNodeCursor extends TraceableCursorImpl<DefaultNodeCursor> implements NodeCursor {
    final StorageNodeCursor storeCursor;
    private final InternalCursorFactory internalCursors;

    protected Read read;
    private TxStateHolder txStateHolder;
    boolean checkHasChanges;
    boolean hasChanges;
    private LongIterator addedNodes;
    private long currentAddedInChunk = NULL;
    private long single;
    private boolean isSingle;

    private final boolean applyAccessModeToTxState;
    private AccessModeProvider accessModeProvider;
    private AccessMode accessMode;
    private AccessControlDataProvider accessControlDataProvider;
    private DefaultRelationshipTraversalCursor securityRelationshipTraversalCursor;
    boolean includeChangesFromThisTransaction = true;

    protected DefaultNodeCursor(
            CursorPool<DefaultNodeCursor> pool,
            StorageNodeCursor storeCursor,
            InternalCursorFactory internalCursors,
            boolean applyAccessModeToTxState) {
        super(pool);
        this.storeCursor = storeCursor;
        this.internalCursors = internalCursors;
        this.applyAccessModeToTxState = applyAccessModeToTxState;
    }

    void scan(
            Read read,
            TxStateHolder txStateHolder,
            AccessModeProvider accessModeProvider,
            boolean includeChangesFromThisTransaction) {
        storeCursor.scan(includeChangesFromThisTransaction);
        this.read = read;
        this.includeChangesFromThisTransaction = includeChangesFromThisTransaction;
        this.txStateHolder = includeChangesFromThisTransaction ? txStateHolder : TxStateHolder.EMPTY_TX_STATE;
        this.accessModeProvider = accessModeProvider;
        this.accessMode = accessModeProvider.getAccessMode();
        this.isSingle = false;
        this.currentAddedInChunk = NULL;
        this.checkHasChanges = true;
        this.addedNodes = ImmutableEmptyLongIterator.INSTANCE;
        if (tracer != null) {
            tracer.onAllNodesScan();
        }
    }

    boolean scanBatch(
            Read read,
            AllNodeScan scan,
            long sizeHint,
            TxStateHolder txStateHolder,
            AccessModeProvider accessModeProvider) {
        this.read = read;
        this.txStateHolder = txStateHolder;
        this.accessModeProvider = accessModeProvider;
        this.accessMode = accessModeProvider.getAccessMode();
        this.isSingle = false;
        this.currentAddedInChunk = NULL;
        this.checkHasChanges = false;
        this.hasChanges = false;
        this.addedNodes = ImmutableEmptyLongIterator.INSTANCE;
        this.includeChangesFromThisTransaction = true; // For now all usages see changes from this transaction.
        return storeCursor.scanBatch(scan, sizeHint);
    }

    void single(long reference, Read read, TxStateHolder txStateHolder, AccessModeProvider accessModeProvider) {
        single(reference, read, txStateHolder, accessModeProvider, true);
    }

    void single(
            long reference,
            Read read,
            TxStateHolder txStateHolder,
            AccessModeProvider accessModeProvider,
            boolean includeChangesFromThisTransaction) {
        storeCursor.single(reference);
        this.read = read;
        this.includeChangesFromThisTransaction = includeChangesFromThisTransaction;
        this.txStateHolder = includeChangesFromThisTransaction ? txStateHolder : TxStateHolder.EMPTY_TX_STATE;
        this.accessModeProvider = accessModeProvider;
        this.accessMode = accessModeProvider.getAccessMode();
        this.single = reference;
        this.isSingle = true;
        this.currentAddedInChunk = NULL;
        this.checkHasChanges = true;
        this.addedNodes = ImmutableEmptyLongIterator.INSTANCE;
    }

    protected boolean currentNodeIsAddedInChunk() {
        return currentAddedInChunk != NULL;
    }

    @Override
    public long nodeReference() {
        if (currentAddedInChunk != NULL) {
            // Special case where the most recent next() call selected a node that exists only in tx-state.
            // Internal methods getting data about this node will also check tx-state and get the data from there.
            return currentAddedInChunk;
        }
        return storeCursor.entityReference();
    }

    @Override
    public TokenSet labels() {
        return labels(storeCursor);
    }

    @Override
    public TokenSet labelsAndProperties(PropertyCursor propertyCursor, PropertySelection selection) {
        if (currentAddedInChunk != NULL) {
            // Node added in tx-state, no reason to go down to store and check
            TransactionState txState = txStateHolder.txState();
            properties(propertyCursor, selection);
            return Labels.from(
                    txState.nodeStateLabelDiffSets(currentAddedInChunk).getAdded());
        } else if (hasChanges()) {
            TransactionState txState = txStateHolder.txState();
            final MutableIntSet labels = new IntHashSet(storeCursor.labels());
            properties(propertyCursor, selection);
            // Augment what was found in store with what we have in tx state
            return Labels.from(txState.augmentLabels(labels, txState.getNodeState(storeCursor.entityReference())));
        } else {
            // Nothing in tx state, just read the data.
            return readLabelsAndProperties(propertyCursor, selection);
        }
    }

    protected TokenSet readLabelsAndProperties(PropertyCursor propertyCursor, PropertySelection selection) {
        var defaultPropertyCursor = (DefaultPropertyCursor) propertyCursor;
        int[] labels = storeCursor.labelsAndProperties(defaultPropertyCursor.storeCursor, selection);
        defaultPropertyCursor.initNode(this, selection, read, false, txStateHolder, accessModeProvider);
        return Labels.from(labels);
    }

    /**
     * The normal labels() method takes into account TxState for both created nodes and set/remove labels.
     * Some code paths need to consider created, but not changed labels.
     */
    @Override
    public TokenSet labelsIgnoringTxStateSetRemove() {
        if (currentAddedInChunk != NULL) {
            // Node added in tx-state, no reason to go down to store and check
            TransactionState txState = txStateHolder.txState();
            return Labels.from(
                    txState.nodeStateLabelDiffSets(currentAddedInChunk).getAdded());
        } else {
            // Nothing in tx state, just read the data.
            return Labels.from(storeCursor.labels());
        }
    }

    @Override
    public boolean hasLabel(int label) {
        if (tracer != null) {
            tracer.onHasLabel(label);
        }

        if (hasChanges()) {
            TransactionState txState = txStateHolder.txState();
            IntDiffSets diffSets = txState.nodeStateLabelDiffSets(nodeReference());
            if (diffSets.isAdded(label)) {
                return true;
            }
            if (currentNodeIsAddedInChunk() || diffSets.isRemoved(label)) {
                return false;
            }
        }

        return storeCursor.hasLabel(label);
    }

    @Override
    public boolean hasLabel() {
        if (tracer != null) {
            tracer.onHasLabel();
        }
        if (hasChanges()) {
            TransactionState txState = txStateHolder.txState();
            IntDiffSets diffSets = txState.nodeStateLabelDiffSets(nodeReference());
            if (diffSets.getAdded().notEmpty()) {
                return true;
            }
            if (currentNodeIsAddedInChunk()) {
                return false;
            }
            // If we remove labels in the transaction we need to do a full check so that we don't remove all of the
            // nodes
            if (diffSets.getRemoved().notEmpty()) {
                return labels().numberOfTokens() > 0;
            }
        }

        return storeCursor.hasLabel();
    }

    @Override
    public void relationships(RelationshipTraversalCursor cursor, RelationshipSelection selection) {
        ((DefaultRelationshipTraversalCursor) cursor).init(this, selection, read, txStateHolder, accessModeProvider);
    }

    @Override
    public boolean supportsFastRelationshipsTo() {
        return currentAddedInChunk == NULL && storeCursor.supportsFastRelationshipsTo();
    }

    @Override
    public void relationshipsTo(
            RelationshipTraversalCursor relationships, RelationshipSelection selection, long neighbourNodeReference) {
        ((DefaultRelationshipTraversalCursor) relationships)
                .init(this, selection, neighbourNodeReference, read, txStateHolder, accessModeProvider);
    }

    @Override
    public void properties(PropertyCursor cursor, PropertySelection selection) {
        ((DefaultPropertyCursor) cursor).initNode(this, selection, read, true, txStateHolder, accessModeProvider);
    }

    @Override
    public long relationshipsReference() {
        return currentAddedInChunk != NULL ? NULL : storeCursor.relationshipsReference();
    }

    @Override
    public Reference propertiesReference() {
        return currentAddedInChunk != NULL ? NULL_REFERENCE : storeCursor.propertiesReference();
    }

    @Override
    public boolean supportsFastDegreeLookup() {
        return (currentAddedInChunk != NULL || storeCursor.supportsFastDegreeLookup()) && allowsTraverseAll();
    }

    @Override
    public int[] relationshipTypes() {
        boolean hasChanges = hasChanges();
        NodeState nodeTxState = hasChanges ? txStateHolder.txState().getNodeState(nodeReference()) : null;
        int[] storedTypes = currentAddedInChunk == NULL ? storeCursor.relationshipTypes() : null;
        MutableIntSet types = storedTypes != null ? IntSets.mutable.of(storedTypes) : IntSets.mutable.empty();
        if (nodeTxState != null) {
            types.addAll(nodeTxState.getAddedRelationshipTypes());
        }
        return types.toArray();
    }

    @Override
    public Degrees degrees(RelationshipSelection selection) {
        EagerDegrees degrees = new EagerDegrees();
        fillDegrees(selection, degrees);
        return degrees;
    }

    @Override
    public long degree(RelationshipSelection selection) {
        SingleDegree degrees = new SingleDegree();
        fillDegrees(selection, degrees);
        return degrees.getTotal();
    }

    @Override
    public long degreeWithMax(long maxDegree, RelationshipSelection selection) {
        SingleDegree degrees = new SingleDegree(maxDegree);
        fillDegrees(selection, degrees);
        return Math.min(degrees.getTotal(), maxDegree);
    }

    private void fillDegrees(RelationshipSelection selection, Degrees.Mutator degrees) {
        if (!allowsTraverseAll()) {
            readRestrictedDegrees(selection, degrees);
            return;
        }
        if (hasChanges()) {
            var nodeTxState = txStateHolder.txState().getNodeState(nodeReference());
            if (nodeTxState != null && !nodeTxState.fillDegrees(selection, degrees)) {
                return;
            }
        }
        if (currentNodeIsAddedInChunk()) {
            return;
        }
        storeCursor.degrees(selection, degrees);
    }

    private void readRestrictedDegrees(RelationshipSelection selection, Degrees.Mutator degrees) {
        var cursor = getSecurityRelationshipTraversalCursor();
        relationships(cursor, selection);
        long thisReference = nodeReference();
        while (cursor.next()) {
            int type = cursor.type();
            long source = cursor.sourceNodeReference();
            long target = cursor.targetNodeReference();
            boolean loop = source == target;
            boolean outgoing = !loop && source == thisReference;
            boolean incoming = !loop && !outgoing;
            if (!degrees.add(type, outgoing ? 1 : 0, incoming ? 1 : 0, loop ? 1 : 0)) {
                return;
            }
        }
    }

    private DefaultRelationshipTraversalCursor getSecurityRelationshipTraversalCursor() {
        if (securityRelationshipTraversalCursor == null) {
            securityRelationshipTraversalCursor = internalCursors.allocateRelationshipTraversalCursor();
        }
        return securityRelationshipTraversalCursor;
    }

    private AccessControlDataProvider getSelectedPropertiesProvider() {
        if (accessControlDataProvider == null) {
            accessControlDataProvider = new AccessControlDataProvider(
                    () -> (propertyCursor, selection) -> {
                        if (storeCursor.entityReference() != NULL) {
                            storeCursor.properties(propertyCursor, selection);
                        }
                    },
                    internalCursors,
                    applyAccessModeToTxState,
                    this::txStateProperties,
                    () -> read);
        }
        return accessControlDataProvider;
    }

    private Iterable<StorageProperty> txStateProperties() {
        return txStateHolder.txState().getNodeState(this.nodeReference()).addedProperties();
    }

    private TokenSet labels(StorageNodeCursor nodeCursor) {
        if (currentAddedInChunk != NULL) {
            // Node added in tx-state, no reason to go down to store and check
            TransactionState txState = txStateHolder.txState();
            return Labels.from(
                    txState.nodeStateLabelDiffSets(currentAddedInChunk).getAdded());
        } else if (hasChanges()) {
            TransactionState txState = txStateHolder.txState();
            final MutableIntSet labels = new IntHashSet(nodeCursor.labels());
            // Augment what was found in store with what we have in tx state
            return Labels.from(txState.augmentLabels(labels, txState.getNodeState(nodeCursor.entityReference())));
        } else {
            // Nothing in tx state, just read the data.
            return Labels.from(nodeCursor.labels());
        }
    }

    @Override
    public boolean next() {
        // Check tx state
        boolean hasChanges = hasChanges();

        if (hasChanges) {
            while (addedNodes.hasNext()) {
                currentAddedInChunk = addedNodes.next();
                if (!applyAccessModeToTxState || allowsTraverse()) {
                    traceNode();
                    return true;
                }
            }
            currentAddedInChunk = NULL;
        }

        while (storeCursor.next()) {
            if (!deletedInThisBatch(hasChanges) && allowsTraverse()) {
                traceNode();
                return true;
            }
        }
        return false;
    }

    private boolean deletedInThisBatch(boolean hasChanges) {
        return hasChanges && txStateHolder.txState().nodeIsDeletedInThisBatch(storeCursor.entityReference());
    }

    private void traceNode() {
        if (tracer != null) {
            tracer.onNode(nodeReference());
        }
    }

    protected boolean allowsTraverse() {
        assert accessMode == accessModeProvider.getAccessMode() : "access mode changed while cursor is in use";
        return accessMode.allowsTraverseNode(
                () -> AccessControlDataProvider.nodeLabels(this, applyAccessModeToTxState),
                getSelectedPropertiesProvider());
    }

    protected boolean allowsTraverseAll() {
        assert accessMode == accessModeProvider.getAccessMode() : "access mode changed while cursor is in use";
        return accessMode.allowsTraverseAllRelTypes() && accessMode.allowsTraverseAllLabels();
    }

    @Override
    public void closeInternal() {
        if (!isClosed()) {
            read = null;
            txStateHolder = null;
            accessModeProvider = null;
            accessMode = null;
            checkHasChanges = true;
            addedNodes = ImmutableEmptyLongIterator.INSTANCE;
            storeCursor.reset();
            if (securityRelationshipTraversalCursor != null) {
                securityRelationshipTraversalCursor.close();
                securityRelationshipTraversalCursor = null;
            }
            if (accessControlDataProvider != null) {
                accessControlDataProvider.close();
                accessControlDataProvider = null;
            }
        }
        super.closeInternal();
    }

    @Override
    public boolean isClosed() {
        return read == null;
    }

    /**
     * NodeCursor should only see changes that are there from the beginning
     * otherwise it will not be stable.
     */
    boolean hasChanges() {
        if (checkHasChanges) {
            hasChanges = txStateHolder.hasTxStateWithChanges();
            if (hasChanges) {
                addedNodes = collectTxStateChanges(txStateHolder);
            }
            checkHasChanges = false;
        }
        return hasChanges;
    }

    private LongIterator collectTxStateChanges(TxStateHolder stateHolder) {
        if (this.isSingle) {
            return stateHolder.txState().nodeIsAddedInThisBatch(single)
                    ? PrimitiveLongCollections.single(single)
                    : ImmutableEmptyLongIterator.INSTANCE;
        }
        return stateHolder.txState().addedAndRemovedNodes().getAdded().freeze().longIterator();
    }

    @Override
    public String toString() {
        if (isClosed()) {
            return "NodeCursor[closed state]";
        }
        return "NodeCursor[id=" + nodeReference() + ", " + storeCursor + "]";
    }

    @Override
    public void release() {
        final var localSecurityRelationshipTraversalCursor = securityRelationshipTraversalCursor;
        final var localAccessControlDataProvider = accessControlDataProvider;
        try (localSecurityRelationshipTraversalCursor;
                localAccessControlDataProvider;
                storeCursor) {
            // A concise and low-cost way of closing all these cursors w/o the overhead of, say IOUtils.closeAll
            if (localSecurityRelationshipTraversalCursor != null) {
                localSecurityRelationshipTraversalCursor.release();
            }
            if (localAccessControlDataProvider != null) {
                localAccessControlDataProvider.release();
            }
        } finally {
            securityRelationshipTraversalCursor = null;
        }
    }
}
