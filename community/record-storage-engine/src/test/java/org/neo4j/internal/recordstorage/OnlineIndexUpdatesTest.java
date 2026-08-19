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
package org.neo4j.internal.recordstorage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.common.EntityType.RELATIONSHIP;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.NODE_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.PROPERTY_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.RELATIONSHIP_CURSOR;
import static org.neo4j.internal.schema.SchemaDescriptors.forSemanticSearch;
import static org.neo4j.io.IOUtils.closeAllUnchecked;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.kernel.impl.store.StoreType.PROPERTY_ARRAY;
import static org.neo4j.kernel.impl.store.StoreType.PROPERTY_STRING;
import static org.neo4j.kernel.impl.store.record.Record.NO_LABELS_FIELD;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_PROPERTY;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_RELATIONSHIP;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.internal.batchimport.cache.NumberArrayFactories;
import org.neo4j.internal.counts.GBPTreeCountsStore;
import org.neo4j.internal.counts.GBPTreeGenericCountsStore;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.recordstorage.Command.NodeCommand;
import org.neo4j.internal.recordstorage.Command.PropertyCommand;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaCache;
import org.neo4j.internal.schema.SemanticSearchSchemaDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.DatabaseCreationOptions;
import org.neo4j.kernel.impl.store.CountsComputer;
import org.neo4j.kernel.impl.store.DynamicAllocatorProvider;
import org.neo4j.kernel.impl.store.DynamicAllocatorProviders;
import org.neo4j.kernel.impl.store.InlineNodeLabels;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.recovery.RecoveryStartupChecker;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.EagerValueIndexEntryUpdate;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.IndexUpdateListener;
import org.neo4j.storageengine.api.StandardConstraintRuleAccessor;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.util.IndexUpdatesWorkSync;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

@PageCacheExtension
@Neo4jLayoutExtension
class OnlineIndexUpdatesTest {
    private static final LogCommandSerialization LATEST_LOG_SERIALIZATION =
            RecordStorageCommandReaderFactory.INSTANCE.get(LatestVersions.LATEST_KERNEL_VERSION);

    private static final int ENTITY_TOKEN = 1;
    private static final int OTHER_ENTITY_TOKEN = 2;
    private static final int[] ENTITY_TOKENS = {ENTITY_TOKEN};

    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private PageCache pageCache;

    @Inject
    private RecordDatabaseLayout databaseLayout;

    private NodeStore nodeStore;
    private RelationshipStore relationshipStore;
    private SchemaCache schemaCache;
    private PropertyPhysicalToLogicalConverter propertyPhysicalToLogicalConverter;
    private NeoStores neoStores;
    private LifeSupport life;
    private DirectRecordAccess<PropertyRecord, PrimitiveRecord> recordAccess;
    private StoreCursors storeCursors;
    private DynamicAllocatorProvider allocatorProvider;
    private List<IndexEntryUpdate> observedIndexUpdates;
    private IndexUpdatesWorkSync indexUpdatesSync;

    @BeforeEach
    void setUp() throws IOException {
        life = new LifeSupport();
        Config config = Config.defaults();
        NullLogProvider nullLogProvider = NullLogProvider.getInstance();
        var pageCacheTracer = NULL;
        CursorContextFactory contextFactory = new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER);
        StoreFactory storeFactory = new StoreFactory(
                databaseLayout,
                config,
                new DefaultIdGeneratorFactory(
                        fileSystem, immediate(), pageCacheTracer, databaseLayout.getDatabaseName()),
                pageCache,
                pageCacheTracer,
                fileSystem,
                nullLogProvider,
                contextFactory,
                false,
                DatabaseCreationOptions.EMPTY_CREATION_OPTIONS);

        neoStores = storeFactory.openAllNeoStores();
        allocatorProvider = DynamicAllocatorProviders.nonTransactionalAllocator(neoStores);

        var counts = new GBPTreeCountsStore(
                pageCache,
                databaseLayout.countStore(),
                fileSystem,
                immediate(),
                new CountsComputer(neoStores, BASE_TX_ID, contextFactory, INSTANCE, NumberArrayFactories.OFF_HEAP),
                false,
                GBPTreeGenericCountsStore.NO_MONITOR,
                databaseLayout.getDatabaseName(),
                1_000,
                NullLogProvider.getInstance(),
                contextFactory,
                pageCacheTracer,
                neoStores.getOpenOptions(),
                RecoveryStartupChecker.EMPTY_CHECKER);
        life.add(wrapInLifecycle(counts));
        nodeStore = neoStores.getNodeStore();
        relationshipStore = neoStores.getRelationshipStore();
        PropertyStore propertyStore = neoStores.getPropertyStore();

        schemaCache = new SchemaCache(
                new StandardConstraintRuleAccessor(),
                (index, indexingBehaviour) -> index,
                new RecordStorageIndexingBehaviour(
                        nodeStore.getRecordsPerPage(), relationshipStore.getRecordsPerPage()));
        storeCursors = new CachedStoreCursors(neoStores, NULL_CONTEXT);
        propertyPhysicalToLogicalConverter = new PropertyPhysicalToLogicalConverter(
                neoStores.getPropertyStore(), storeCursors, EmptyMemoryTracker.INSTANCE);
        life.start();
        recordAccess = new DirectRecordAccess<>(
                neoStores.getPropertyStore(),
                Loaders.propertyLoader(propertyStore, storeCursors),
                NULL_CONTEXT,
                PROPERTY_CURSOR,
                storeCursors,
                EmptyMemoryTracker.INSTANCE);
        observedIndexUpdates = new ArrayList<>();
        indexUpdatesSync = new IndexUpdatesWorkSync(
                new IndexUpdateListener.Adapter() {
                    @Override
                    public void applyUpdates(
                            Iterator<IndexEntryUpdate> updates, CursorContext cursorContext, boolean parallel) {
                        updates.forEachRemaining(observedIndexUpdates::add);
                    }
                },
                false);
    }

    @AfterEach
    void tearDown() {
        life.shutdown();
        closeAllUnchecked(storeCursors, neoStores);
    }

    @Test
    void shouldContainFedNodeUpdate() throws IOException {
        OnlineIndexUpdates onlineIndexUpdates = new OnlineIndexUpdates(
                nodeStore,
                schemaCache,
                propertyPhysicalToLogicalConverter,
                new RecordStorageReader(neoStores),
                NULL_CONTEXT,
                INSTANCE,
                storeCursors,
                indexUpdatesSync);

        int nodeId = 0;
        NodeRecord inUse = getNode(nodeId, true);
        Value propertyValue = Values.of("hej");
        long propertyId = createProperty(inUse, propertyValue, 1);
        NodeRecord notInUse = getNode(nodeId, false);
        try (var nodeCursor = storeCursors.writeCursor(NODE_CURSOR)) {
            nodeStore.updateRecord(inUse, nodeCursor, NULL_CONTEXT, storeCursors);
        }

        NodeCommand nodeCommand = new NodeCommand(LATEST_LOG_SERIALIZATION, inUse, notInUse);
        PropertyRecord propertyBlocks = new PropertyRecord(propertyId);
        propertyBlocks.setNodeId(nodeId);
        PropertyCommand propertyCommand = new PropertyCommand(
                LATEST_LOG_SERIALIZATION, recordAccess.getIfLoaded(propertyId).forReadingData(), propertyBlocks);

        IndexDescriptor indexDescriptor = IndexPrototype.forSchema(
                        forSemanticSearch(NODE, ENTITY_TOKENS, new int[] {1, 4, 6}))
                .withName("index")
                .materialise(0);
        createIndexes(indexDescriptor);

        onlineIndexUpdates.feed(
                nodeGroup(nodeCommand, propertyCommand), relationshipGroup(null), CommandSelector.NORMAL);
        // Tests that apply() close "pending" updates
        for (int i = 0; i < 2; i++) {
            onlineIndexUpdates.apply();
            assertThat(observedIndexUpdates)
                    .containsExactly(
                            EagerValueIndexEntryUpdate.remove(nodeId, indexDescriptor, propertyValue, null, null));
        }
    }

    @Test
    void shouldContainFedRelationshipUpdate() throws IOException {
        OnlineIndexUpdates onlineIndexUpdates = new OnlineIndexUpdates(
                nodeStore,
                schemaCache,
                propertyPhysicalToLogicalConverter,
                new RecordStorageReader(neoStores),
                NULL_CONTEXT,
                INSTANCE,
                storeCursors,
                indexUpdatesSync);

        long relId = 0;
        RelationshipRecord inUse = getRelationship(relId, true, ENTITY_TOKEN);
        Value propertyValue = Values.of("hej");
        long propertyId = createProperty(inUse, propertyValue, 1);
        RelationshipRecord notInUse = getRelationship(relId, false, ENTITY_TOKEN);
        try (PageCursor pageCursor = storeCursors.writeCursor(RELATIONSHIP_CURSOR)) {
            relationshipStore.updateRecord(inUse, pageCursor, NULL_CONTEXT, storeCursors);
        }

        Command.RelationshipCommand relationshipCommand =
                new Command.RelationshipCommand(LATEST_LOG_SERIALIZATION, inUse, notInUse);
        PropertyRecord propertyBlocks = new PropertyRecord(propertyId);
        propertyBlocks.setRelId(relId);
        PropertyCommand propertyCommand = new PropertyCommand(
                LATEST_LOG_SERIALIZATION, recordAccess.getIfLoaded(propertyId).forReadingData(), propertyBlocks);

        IndexDescriptor indexDescriptor = IndexPrototype.forSchema(
                        forSemanticSearch(RELATIONSHIP, ENTITY_TOKENS, new int[] {1, 4, 6}))
                .withName("index")
                .materialise(0);
        createIndexes(indexDescriptor);

        onlineIndexUpdates.feed(
                nodeGroup(null), relationshipGroup(relationshipCommand, propertyCommand), CommandSelector.NORMAL);
        // Tests that apply() close "pending" updates
        for (int i = 0; i < 2; i++) {
            onlineIndexUpdates.apply();
            assertThat(observedIndexUpdates)
                    .containsExactly(
                            EagerValueIndexEntryUpdate.remove(relId, indexDescriptor, propertyValue, null, null));
        }
    }

    @Test
    void shouldDifferentiateNodesAndRelationships() throws IOException {
        OnlineIndexUpdates onlineIndexUpdates = new OnlineIndexUpdates(
                nodeStore,
                schemaCache,
                propertyPhysicalToLogicalConverter,
                new RecordStorageReader(neoStores),
                NULL_CONTEXT,
                INSTANCE,
                storeCursors,
                indexUpdatesSync);

        int nodeId = 0;
        NodeRecord inUseNode = getNode(nodeId, true);
        Value nodePropertyValue = Values.of("hej");
        long nodePropertyId = createProperty(inUseNode, nodePropertyValue, 1);
        NodeRecord notInUseNode = getNode(nodeId, false);
        try (PageCursor pageCursor = storeCursors.writeCursor(NODE_CURSOR)) {
            nodeStore.updateRecord(inUseNode, pageCursor, NULL_CONTEXT, storeCursors);
        }

        NodeCommand nodeCommand = new NodeCommand(LATEST_LOG_SERIALIZATION, inUseNode, notInUseNode);
        PropertyRecord nodePropertyBlocks = new PropertyRecord(nodePropertyId);
        nodePropertyBlocks.setNodeId(nodeId);
        PropertyCommand nodePropertyCommand = new PropertyCommand(
                LATEST_LOG_SERIALIZATION,
                recordAccess.getIfLoaded(nodePropertyId).forReadingData(),
                nodePropertyBlocks);

        IndexDescriptor nodeIndexDescriptor = IndexPrototype.forSchema(
                        forSemanticSearch(NODE, ENTITY_TOKENS, new int[] {1, 4, 6}))
                .withName("index")
                .materialise(0);
        createIndexes(nodeIndexDescriptor);

        long relId = 0;
        RelationshipRecord inUse = getRelationship(relId, true, ENTITY_TOKEN);
        Value relationshipPropertyValue = Values.of("da");
        long propertyId = createProperty(inUse, relationshipPropertyValue, 1);
        RelationshipRecord notInUse = getRelationship(relId, false, ENTITY_TOKEN);
        try (PageCursor pageCursor = storeCursors.writeCursor(RELATIONSHIP_CURSOR)) {
            relationshipStore.updateRecord(inUse, pageCursor, NULL_CONTEXT, storeCursors);
        }

        Command.RelationshipCommand relationshipCommand =
                new Command.RelationshipCommand(LATEST_LOG_SERIALIZATION, inUse, notInUse);
        PropertyRecord relationshipPropertyBlocks = new PropertyRecord(propertyId);
        relationshipPropertyBlocks.setRelId(relId);
        PropertyCommand relationshipPropertyCommand = new PropertyCommand(
                LATEST_LOG_SERIALIZATION,
                recordAccess.getIfLoaded(propertyId).forReadingData(),
                relationshipPropertyBlocks);

        SemanticSearchSchemaDescriptor schema = forSemanticSearch(RELATIONSHIP, ENTITY_TOKENS, new int[] {1, 4, 6});
        IndexDescriptor relationshipIndexDescriptor =
                IndexPrototype.forSchema(schema).withName("index").materialise(1);
        createIndexes(relationshipIndexDescriptor);

        onlineIndexUpdates.feed(
                nodeGroup(nodeCommand, nodePropertyCommand),
                relationshipGroup(relationshipCommand, relationshipPropertyCommand),
                CommandSelector.NORMAL);
        // Tests that apply() close "pending" updates
        for (int i = 0; i < 2; i++) {
            onlineIndexUpdates.apply();
            assertThat(observedIndexUpdates)
                    .containsExactly(
                            EagerValueIndexEntryUpdate.remove(
                                    nodeId, nodeIndexDescriptor, nodePropertyValue, null, null),
                            EagerValueIndexEntryUpdate.remove(
                                    relId, relationshipIndexDescriptor, relationshipPropertyValue, null, null));
        }
    }

    @Test
    void shouldUpdateCorrectIndexes() throws IOException {
        OnlineIndexUpdates onlineIndexUpdates = new OnlineIndexUpdates(
                nodeStore,
                schemaCache,
                propertyPhysicalToLogicalConverter,
                new RecordStorageReader(neoStores),
                NULL_CONTEXT,
                INSTANCE,
                storeCursors,
                indexUpdatesSync);

        long relId = 0;
        RelationshipRecord inUse = getRelationship(relId, true, ENTITY_TOKEN);
        Value propertyValue = Values.of("hej");
        Value propertyValue2 = Values.of("da");
        long propertyId = createProperty(inUse, propertyValue, 1);
        long propertyId2 = createProperty(inUse, propertyValue2, 4);
        RelationshipRecord notInUse = getRelationship(relId, false, ENTITY_TOKEN);
        try (PageCursor pageCursor = storeCursors.writeCursor(RELATIONSHIP_CURSOR)) {
            relationshipStore.updateRecord(inUse, pageCursor, NULL_CONTEXT, storeCursors);
        }

        Command.RelationshipCommand relationshipCommand =
                new Command.RelationshipCommand(LATEST_LOG_SERIALIZATION, inUse, notInUse);
        PropertyRecord propertyBlocks = new PropertyRecord(propertyId);
        propertyBlocks.setRelId(relId);
        PropertyCommand propertyCommand = new PropertyCommand(
                LATEST_LOG_SERIALIZATION, recordAccess.getIfLoaded(propertyId).forReadingData(), propertyBlocks);

        PropertyRecord propertyBlocks2 = new PropertyRecord(propertyId2);
        propertyBlocks2.setRelId(relId);
        PropertyCommand propertyCommand2 = new PropertyCommand(
                LATEST_LOG_SERIALIZATION, recordAccess.getIfLoaded(propertyId2).forReadingData(), propertyBlocks2);

        IndexDescriptor indexDescriptor0 = IndexPrototype.forSchema(
                        forSemanticSearch(RELATIONSHIP, ENTITY_TOKENS, new int[] {1, 4, 6}))
                .withName("index_0")
                .materialise(0);
        IndexDescriptor indexDescriptor1 = IndexPrototype.forSchema(
                        forSemanticSearch(RELATIONSHIP, ENTITY_TOKENS, new int[] {2, 4, 6}))
                .withName("index_1")
                .materialise(1);
        IndexDescriptor indexDescriptor = IndexPrototype.forSchema(
                        forSemanticSearch(RELATIONSHIP, new int[] {ENTITY_TOKEN, OTHER_ENTITY_TOKEN}, new int[] {1}))
                .withName("index_2")
                .materialise(2);
        createIndexes(indexDescriptor0, indexDescriptor1, indexDescriptor);

        onlineIndexUpdates.feed(
                nodeGroup(null),
                relationshipGroup(relationshipCommand, propertyCommand, propertyCommand2),
                CommandSelector.NORMAL);
        // Tests that apply() close "pending" updates
        for (int i = 0; i < 2; i++) {
            onlineIndexUpdates.apply();
            assertThat(observedIndexUpdates)
                    .containsExactly(
                            EagerValueIndexEntryUpdate.remove(
                                    relId, indexDescriptor0, propertyValue, propertyValue2, null),
                            EagerValueIndexEntryUpdate.remove(relId, indexDescriptor1, null, propertyValue2, null),
                            EagerValueIndexEntryUpdate.remove(relId, indexDescriptor, propertyValue));
        }
    }

    private void createIndexes(IndexDescriptor... indexDescriptors) {
        for (IndexDescriptor indexDescriptor : indexDescriptors) {
            schemaCache.addSchemaRule(indexDescriptor);
        }
    }

    private EntityCommandGrouper<NodeCommand>.Cursor nodeGroup(
            NodeCommand nodeCommand, PropertyCommand... propertyCommands) {
        return group(nodeCommand, NodeCommand.class, propertyCommands);
    }

    private EntityCommandGrouper<Command.RelationshipCommand>.Cursor relationshipGroup(
            Command.RelationshipCommand relationshipCommand, PropertyCommand... propertyCommands) {
        return group(relationshipCommand, Command.RelationshipCommand.class, propertyCommands);
    }

    private <ENTITY extends Command> EntityCommandGrouper<ENTITY>.Cursor group(
            ENTITY entityCommand, Class<ENTITY> cls, PropertyCommand... propertyCommands) {
        EntityCommandGrouper<ENTITY> grouper = new EntityCommandGrouper<>(cls, 8);
        if (entityCommand != null) {
            grouper.add(entityCommand);
        }
        for (PropertyCommand propertyCommand : propertyCommands) {
            grouper.add(propertyCommand);
        }
        return grouper.sortAndAccessGroups();
    }

    private long createProperty(PrimitiveRecord nodeRecord, Value value, int propertyKey) {
        var propertyStore = neoStores.getPropertyStore();
        var propertyRecord = recordAccess
                .create(propertyStore.getIdGenerator().nextId(NULL_CONTEXT), nodeRecord, NULL_CONTEXT)
                .forChangingData();
        propertyRecord.setInUse(true);
        propertyRecord.setCreated();

        PropertyBlock propertyBlock = new PropertyBlock();
        PropertyStore.encodeValue(
                propertyBlock,
                propertyKey,
                value,
                allocatorProvider.allocator(PROPERTY_STRING),
                allocatorProvider.allocator(PROPERTY_ARRAY),
                NULL_CONTEXT,
                INSTANCE,
                "db-format-2000");
        propertyRecord.addPropertyBlock(propertyBlock);

        return propertyRecord.getId();
    }

    private static NodeRecord getNode(int nodeId, boolean inUse) {
        NodeRecord nodeRecord = new NodeRecord(nodeId);
        nodeRecord = nodeRecord.initialize(
                inUse,
                NO_NEXT_PROPERTY.longValue(),
                false,
                NO_NEXT_RELATIONSHIP.longValue(),
                NO_LABELS_FIELD.longValue());
        if (inUse) {
            InlineNodeLabels labelFieldWriter = new InlineNodeLabels(nodeRecord);
            labelFieldWriter.put(new int[] {ENTITY_TOKEN}, null, null, NULL_CONTEXT, StoreCursors.NULL, INSTANCE);
        }
        return nodeRecord;
    }

    private static RelationshipRecord getRelationship(long relId, boolean inUse, int type) {
        if (!inUse) {
            type = -1;
        }
        return new RelationshipRecord(relId)
                .initialize(
                        inUse,
                        NO_NEXT_PROPERTY.longValue(),
                        0,
                        0,
                        type,
                        NO_NEXT_RELATIONSHIP.longValue(),
                        NO_NEXT_RELATIONSHIP.longValue(),
                        NO_NEXT_RELATIONSHIP.longValue(),
                        NO_NEXT_RELATIONSHIP.longValue(),
                        true,
                        false,
                        false,
                        false);
    }

    private static Lifecycle wrapInLifecycle(GBPTreeCountsStore countsStore) {
        return new LifecycleAdapter() {
            @Override
            public void start() throws IOException {
                countsStore.start(NULL_CONTEXT, INSTANCE);
            }

            @Override
            public void shutdown() {
                countsStore.close();
            }
        };
    }
}
