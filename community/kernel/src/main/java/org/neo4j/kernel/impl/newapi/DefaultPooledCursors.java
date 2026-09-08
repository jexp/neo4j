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

import java.util.ArrayList;
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor;
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.lang.AutoCloseablePlus;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StorageRelationshipScanCursor;
import org.neo4j.storageengine.api.cursor.StoreCursors;

/**
 * Cursor factory which pools 1 cursor of each kind. Not thread-safe at all.
 */
public class DefaultPooledCursors extends DefaultCursors implements CursorFactory {
    private static final int NODE = 0;
    private static final int FULL_NODE = 1;
    private static final int RELATIONSHIP_SCAN = 2;
    private static final int FULL_RELATIONSHIP_SCAN = 3;
    private static final int RELATIONSHIP_TRAVERSAL = 4;
    private static final int FULL_RELATIONSHIP_TRAVERSAL = 5;
    private static final int PROPERTY = 6;
    private static final int FULL_PROPERTY = 7;
    private static final int NODE_VALUE_INDEX = 8;
    private static final int NODE_LABEL_INDEX = 9;
    private static final int FULL_NODE_LABEL_INDEX = 10;
    private static final int RELATIONSHIP_VALUE_INDEX = 11;
    private static final int RELATIONSHIP_TYPE_INDEX = 12;
    private static final int FULL_RELATIONSHIP_TYPE_INDEX = 13;

    private final StorageReader storageReader;
    private final StoreCursors storeCursors;
    private final StorageEngineIndexingBehaviour indexingBehaviour;
    private final boolean applyAccessModeToTxState;
    private final DefaultCursorFactory cursorFactory;
    private DefaultNodeCursor nodeCursor;
    private DefaultNodeCursor fullAccessNodeCursor;
    private DefaultRelationshipScanCursor relationshipScanCursor;
    private DefaultRelationshipScanCursor fullAccessRelationshipScanCursor;
    private DefaultRelationshipTraversalCursor relationshipTraversalCursor;
    private DefaultRelationshipTraversalCursor fullAccessRelationshipTraversalCursor;
    private TraceablePropertyCursor propertyCursor;
    private PropertyCursor fullAccessPropertyCursor;
    private DefaultNodeValueIndexCursor nodeValueIndexCursor;
    private DefaultNodeLabelIndexCursor nodeLabelIndexCursor;
    private DefaultRelationshipValueIndexCursor relationshipValueIndexCursor;
    private DefaultNodeLabelIndexCursor fullAccessNodeLabelIndexCursor;
    private InternalRelationshipTypeIndexCursor relationshipTypeIndexCursor;
    private InternalRelationshipTypeIndexCursor fullAccessRelationshipTypeIndexCursor;

    public DefaultPooledCursors(
            StorageReader storageReader,
            StoreCursors storeCursors,
            Config config,
            StorageEngineIndexingBehaviour indexingBehaviour,
            boolean applyAccessModeToTxState) {
        this(
                storageReader,
                storeCursors,
                config,
                indexingBehaviour,
                applyAccessModeToTxState,
                DefaultCursorFactory.DEFAULT);
    }

    protected DefaultPooledCursors(
            StorageReader storageReader,
            StoreCursors storeCursors,
            Config config,
            StorageEngineIndexingBehaviour indexingBehaviour,
            boolean applyAccessModeToTxState,
            DefaultCursorFactory cursorFactory) {
        super(new ArrayList<>(), config);
        this.storageReader = storageReader;
        this.storeCursors = storeCursors;
        this.indexingBehaviour = indexingBehaviour;
        this.applyAccessModeToTxState = applyAccessModeToTxState;
        this.cursorFactory = cursorFactory;
    }

    private <T extends AutoCloseablePlus> T trace(T cursor, int ignored) {
        return trace(cursor);
    }

    @Override
    public DefaultNodeCursor allocateNodeCursor(CursorContext cursorContext, MemoryTracker memoryTracker) {
        if (nodeCursor == null) {
            return trace(newNodeCursor(cursorContext, memoryTracker), NODE);
        }
        try {
            return acquire(nodeCursor);
        } finally {
            nodeCursor = null;
        }
    }

    private DefaultNodeCursor newNodeCursor(CursorContext cursorContext, MemoryTracker memoryTracker) {
        return cursorFactory.nodeCursor(
                this::accept,
                storageReader.allocateNodeCursor(cursorContext, storeCursors, memoryTracker),
                newInternalCursors(cursorContext, memoryTracker),
                applyAccessModeToTxState);
    }

    private <T extends TraceableCursor> T acceptCursor(T pooledCursor, T cursor) {
        if (pooledCursor != null) {
            pooledCursor.release();
        }
        cursor.removeTracer();
        // TODO trace and warn/log on accepting more cursors than allocating
        return cursor;
    }

    protected void accept(DefaultNodeCursor cursor) {
        nodeCursor = acceptCursor(nodeCursor, cursor);
    }

    @Override
    public DefaultNodeCursor allocateFullAccessNodeCursor(CursorContext cursorContext, MemoryTracker memoryTracker) {
        if (fullAccessNodeCursor == null) {
            return trace(
                    cursorFactory.fullAccessNodeCursor(
                            this::acceptFullAccess,
                            storageReader.allocateNodeCursor(cursorContext, storeCursors, memoryTracker)),
                    FULL_NODE);
        }
        try {
            return acquire(fullAccessNodeCursor);
        } finally {
            fullAccessNodeCursor = null;
        }
    }

    private void acceptFullAccess(DefaultNodeCursor cursor) {
        fullAccessNodeCursor = acceptCursor(fullAccessNodeCursor, cursor);
    }

    @Override
    public RelationshipScanCursor allocateRelationshipScanCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        if (relationshipScanCursor == null) {
            StorageRelationshipScanCursor storeCursor =
                    storageReader.allocateRelationshipScanCursor(cursorContext, storeCursors, memoryTracker);
            InternalCursorFactory internalCursors = newInternalCursors(cursorContext, memoryTracker);
            return trace(
                    cursorFactory.relationshipScanCursor(
                            this::accept, storeCursor, internalCursors, applyAccessModeToTxState),
                    RELATIONSHIP_SCAN);
        }
        try {
            return acquire(relationshipScanCursor);
        } finally {
            relationshipScanCursor = null;
        }
    }

    private void accept(DefaultRelationshipScanCursor cursor) {
        relationshipScanCursor = acceptCursor(relationshipScanCursor, cursor);
    }

    @Override
    public DefaultRelationshipScanCursor allocateFullAccessRelationshipScanCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        if (fullAccessRelationshipScanCursor == null) {
            return trace(
                    cursorFactory.fullAccessRelationshipScanCursor(
                            this::acceptFullAccess,
                            storageReader.allocateRelationshipScanCursor(cursorContext, storeCursors, memoryTracker)),
                    FULL_RELATIONSHIP_SCAN);
        }

        try {
            return acquire(fullAccessRelationshipScanCursor);
        } finally {
            fullAccessRelationshipScanCursor = null;
        }
    }

    private <C extends TraceableCursor> C acquire(C cursor) {
        cursor.acquire();
        return cursor;
    }

    private void acceptFullAccess(DefaultRelationshipScanCursor cursor) {
        fullAccessRelationshipScanCursor = acceptCursor(fullAccessRelationshipScanCursor, cursor);
    }

    @Override
    public DefaultRelationshipTraversalCursor allocateRelationshipTraversalCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        if (relationshipTraversalCursor == null) {
            return trace(
                    cursorFactory.relationshipTraversalCursor(
                            this::accept,
                            storageReader.allocateRelationshipTraversalCursor(
                                    cursorContext, storeCursors, memoryTracker),
                            newInternalCursors(cursorContext, memoryTracker),
                            applyAccessModeToTxState),
                    RELATIONSHIP_TRAVERSAL);
        }

        try {
            return acquire(relationshipTraversalCursor);
        } finally {
            relationshipTraversalCursor = null;
        }
    }

    private void accept(DefaultRelationshipTraversalCursor cursor) {
        relationshipTraversalCursor = acceptCursor(relationshipTraversalCursor, cursor);
    }

    @Override
    public RelationshipTraversalCursor allocateFullAccessRelationshipTraversalCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        if (fullAccessRelationshipTraversalCursor == null) {
            return trace(
                    cursorFactory.fullAccessRelationshipTraversalCursor(
                            this::acceptFullAccess,
                            storageReader.allocateRelationshipTraversalCursor(
                                    cursorContext, storeCursors, memoryTracker)),
                    FULL_RELATIONSHIP_TRAVERSAL);
        }

        try {
            return acquire(fullAccessRelationshipTraversalCursor);
        } finally {
            fullAccessRelationshipTraversalCursor = null;
        }
    }

    private void acceptFullAccess(DefaultRelationshipTraversalCursor cursor) {
        fullAccessRelationshipTraversalCursor = acceptCursor(fullAccessRelationshipTraversalCursor, cursor);
    }

    @Override
    public PropertyCursor allocatePropertyCursor(CursorContext cursorContext, MemoryTracker memoryTracker) {
        if (propertyCursor == null) {
            return trace(
                    cursorFactory.propertyCursor(
                            this::accept,
                            storageReader.allocatePropertyCursor(cursorContext, storeCursors, memoryTracker),
                            newInternalCursors(cursorContext, memoryTracker),
                            applyAccessModeToTxState),
                    PROPERTY);
        }
        try {
            return acquire(propertyCursor);
        } finally {
            propertyCursor = null;
        }
    }

    private void accept(TraceablePropertyCursor cursor) {
        propertyCursor = acceptCursor(propertyCursor, cursor);
    }

    @Override
    public PropertyCursor allocateFullAccessPropertyCursor(CursorContext cursorContext, MemoryTracker memoryTracker) {
        if (fullAccessPropertyCursor == null) {
            return trace(
                    cursorFactory.fullAccessPropertyCursor(
                            this::acceptFullAccess,
                            storageReader.allocatePropertyCursor(cursorContext, storeCursors, memoryTracker)),
                    FULL_PROPERTY);
        }

        try {
            return acquire((TraceableCursor & PropertyCursor) fullAccessPropertyCursor);
        } finally {
            fullAccessPropertyCursor = null;
        }
    }

    private void acceptFullAccess(PropertyCursor cursor) {
        fullAccessPropertyCursor = acceptCursor(
                (TraceablePropertyCursor & PropertyCursor) fullAccessPropertyCursor,
                (TraceablePropertyCursor & PropertyCursor) cursor);
    }

    @Override
    public NodeValueIndexCursor allocateNodeValueIndexCursor(CursorContext cursorContext, MemoryTracker memoryTracker) {
        if (nodeValueIndexCursor == null) {
            return trace(
                    cursorFactory.nodeValueIndexCursor(
                            this::accept, newInternalCursors(cursorContext, memoryTracker), applyAccessModeToTxState),
                    NODE_VALUE_INDEX);
        }

        try {
            return acquire(nodeValueIndexCursor);
        } finally {
            nodeValueIndexCursor = null;
        }
    }

    private void accept(DefaultNodeValueIndexCursor cursor) {
        nodeValueIndexCursor = acceptCursor(nodeValueIndexCursor, cursor);
    }

    @Override
    public DefaultNodeLabelIndexCursor allocateNodeLabelIndexCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        if (nodeLabelIndexCursor == null) {
            return trace(
                    cursorFactory.nodeLabelIndexCursor(
                            this::accept, newInternalCursors(cursorContext, memoryTracker), applyAccessModeToTxState),
                    NODE_LABEL_INDEX);
        }

        try {
            return acquire(nodeLabelIndexCursor);
        } finally {
            nodeLabelIndexCursor = null;
        }
    }

    private void accept(DefaultNodeLabelIndexCursor cursor) {
        nodeLabelIndexCursor = acceptCursor(nodeLabelIndexCursor, cursor);
    }

    @Override
    public DefaultNodeLabelIndexCursor allocateFullAccessNodeLabelIndexCursor(CursorContext cursorContext) {
        if (fullAccessNodeLabelIndexCursor == null) {
            return trace(cursorFactory.fullAccessNodeLabelIndexCursor(this::acceptFullAccess), FULL_NODE_LABEL_INDEX);
        }

        try {
            return acquire(fullAccessNodeLabelIndexCursor);
        } finally {
            fullAccessNodeLabelIndexCursor = null;
        }
    }

    private void acceptFullAccess(DefaultNodeLabelIndexCursor cursor) {
        fullAccessNodeLabelIndexCursor = acceptCursor(fullAccessNodeLabelIndexCursor, cursor);
    }

    @Override
    public RelationshipValueIndexCursor allocateRelationshipValueIndexCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        if (relationshipValueIndexCursor == null) {
            var internalCursors = newInternalCursors(cursorContext, memoryTracker);
            StorageRelationshipScanCursor storeCursor =
                    storageReader.allocateRelationshipScanCursor(cursorContext, storeCursors, memoryTracker);
            DefaultRelationshipScanCursor relationshipScanCursor = cursorFactory.relationshipScanCursor(
                    c -> {}, storeCursor, internalCursors, applyAccessModeToTxState);
            return trace(
                    cursorFactory.relationshipValueIndexCursor(
                            this::accept, relationshipScanCursor, internalCursors, applyAccessModeToTxState),
                    RELATIONSHIP_VALUE_INDEX);
        }

        try {
            return acquire(relationshipValueIndexCursor);
        } finally {
            relationshipValueIndexCursor = null;
        }
    }

    public void accept(DefaultRelationshipValueIndexCursor cursor) {
        relationshipValueIndexCursor = acceptCursor(relationshipValueIndexCursor, cursor);
    }

    @Override
    public RelationshipTypeIndexCursor allocateRelationshipTypeIndexCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        if (relationshipTypeIndexCursor == null) {
            var internalCursors = newInternalCursors(cursorContext, memoryTracker);
            if (indexingBehaviour.useNodeIdsInRelationshipTokenIndex()) {
                var relationshipTraversalCursor = cursorFactory.relationshipTraversalCursor(
                        c -> {},
                        storageReader.allocateRelationshipTraversalCursor(cursorContext, storeCursors, memoryTracker),
                        internalCursors,
                        applyAccessModeToTxState);

                return trace(
                        cursorFactory.nodeBasedRelationshipTypeIndexCursor(
                                this::accept, internalCursors.allocateNodeCursor(), relationshipTraversalCursor),
                        RELATIONSHIP_TYPE_INDEX);
            } else {
                var relationshipScanCursor = cursorFactory.relationshipScanCursor(
                        c -> {},
                        storageReader.allocateRelationshipScanCursor(cursorContext, storeCursors, memoryTracker),
                        internalCursors,
                        applyAccessModeToTxState);
                return trace(
                        cursorFactory.relationshipBasedRelationshipTypeIndexCursor(
                                this::accept, relationshipScanCursor, applyAccessModeToTxState),
                        RELATIONSHIP_TYPE_INDEX);
            }
        }

        try {
            return acquire(relationshipTypeIndexCursor);
        } finally {
            relationshipTypeIndexCursor = null;
        }
    }

    private void accept(InternalRelationshipTypeIndexCursor cursor) {
        relationshipTypeIndexCursor = acceptCursor(relationshipTypeIndexCursor, cursor);
    }

    @Override
    public RelationshipTypeIndexCursor allocateFullAccessRelationshipTypeIndexCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        if (fullAccessRelationshipTypeIndexCursor == null) {
            if (indexingBehaviour.useNodeIdsInRelationshipTokenIndex()) {
                var nodeCursor = cursorFactory.fullAccessNodeCursor(
                        c -> {}, storageReader.allocateNodeCursor(cursorContext, storeCursors, memoryTracker));
                var relationshipTraversalCursor = cursorFactory.fullAccessRelationshipTraversalCursor(
                        c -> {},
                        storageReader.allocateRelationshipTraversalCursor(cursorContext, storeCursors, memoryTracker));
                return trace(
                        cursorFactory.fullAccessNodeBasedRelationshipTypeIndexCursor(
                                this::acceptFullAccess, nodeCursor, relationshipTraversalCursor),
                        FULL_RELATIONSHIP_TYPE_INDEX);
            } else {

                var relationshipScanCursor = cursorFactory.fullAccessRelationshipScanCursor(
                        c -> {},
                        storageReader.allocateRelationshipScanCursor(cursorContext, storeCursors, memoryTracker));
                return trace(
                        cursorFactory.fullAccessRelationshipBasedRelationshipTypeIndexCursor(
                                this::acceptFullAccess, relationshipScanCursor),
                        FULL_RELATIONSHIP_TYPE_INDEX);
            }
        }

        try {
            return acquire(fullAccessRelationshipTypeIndexCursor);
        } finally {
            fullAccessRelationshipTypeIndexCursor = null;
        }
    }

    private void acceptFullAccess(InternalRelationshipTypeIndexCursor cursor) {
        fullAccessRelationshipTypeIndexCursor = acceptCursor(fullAccessRelationshipTypeIndexCursor, cursor);
    }

    public void release() {
        if (nodeCursor != null) {
            nodeCursor.release();
        }
        if (fullAccessNodeCursor != null) {
            fullAccessNodeCursor.release();
        }
        if (relationshipScanCursor != null) {
            relationshipScanCursor.release();
        }
        if (fullAccessRelationshipScanCursor != null) {
            fullAccessRelationshipScanCursor.release();
        }
        if (relationshipTraversalCursor != null) {
            relationshipTraversalCursor.release();
        }
        if (fullAccessRelationshipTraversalCursor != null) {
            fullAccessRelationshipTraversalCursor.release();
        }
        if (propertyCursor != null) {
            propertyCursor.release();
        }
        if (fullAccessPropertyCursor != null) {
            ((TraceableCursor) fullAccessPropertyCursor).release();
        }
        if (nodeValueIndexCursor != null) {
            nodeValueIndexCursor.release();
        }
        if (nodeLabelIndexCursor != null) {
            nodeLabelIndexCursor.release();
        }
        if (fullAccessNodeLabelIndexCursor != null) {
            fullAccessNodeLabelIndexCursor.release();
        }
        if (relationshipValueIndexCursor != null) {
            relationshipValueIndexCursor.release();
        }
        if (relationshipTypeIndexCursor != null) {
            relationshipTypeIndexCursor.release();
        }
        if (fullAccessRelationshipTypeIndexCursor != null) {
            fullAccessRelationshipTypeIndexCursor.release();
        }
        nodeCursor = null;
        fullAccessNodeCursor = null;
        relationshipScanCursor = null;
        fullAccessRelationshipScanCursor = null;
        relationshipTraversalCursor = null;
        fullAccessRelationshipTraversalCursor = null;
        propertyCursor = null;
        fullAccessPropertyCursor = null;
        nodeValueIndexCursor = null;
        nodeLabelIndexCursor = null;
        fullAccessNodeLabelIndexCursor = null;
        relationshipValueIndexCursor = null;
        relationshipTypeIndexCursor = null;
        fullAccessRelationshipTypeIndexCursor = null;

        // TODO trace and warn/log on unaccounted cursors
    }

    private InternalCursorFactory newInternalCursors(CursorContext cursorContext, MemoryTracker memoryTracker) {
        return cursorFactory.internalCursors(
                storageReader, storeCursors, cursorContext, memoryTracker, applyAccessModeToTxState);
    }
}
