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
package org.neo4j.index.internal.gbptree;

import static org.neo4j.index.internal.gbptree.PointerChecking.checkOutOfBounds;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.util.Preconditions;
import org.neo4j.util.VisibleForTesting;

/**
 * {@link Seeker} over tree leaves, making keys/values accessible to user. Given a starting leaf
 * and key range this cursor traverses each leaf and its right siblings as long as visited keys are within
 * key range. Each visited key within the key range can be accessible using {@link #key()}.
 * The instances provided by {@link Seeker#key()} and {@link Seeker#value()} are mutable and overwritten with new values
 * for every call to {@link #next()} so user cannot keep references to key/value instances, expecting them
 * to keep their values intact.
 * <p>
 * Concurrent writes can happen in the visited nodes and tree structure may change. This implementation
 * guards for that by re-reading if change happens underneath, but will not provide a consistent view of
 * the data as it were when the seek starts.
 * <p>
 * Seek can be performed forwards or backwards, returning hits in ascending or descending order respectively
 * (as defined by {@link Layout#compare(Object, Object)}). Direction is decided on relation between
 * {@link #fromInclusive} and {@link #toExclusive}. Backwards seek is expected to be slower than forwards seek
 * because extra care needs to be taken to make sure no keys are skipped when keys are moved to the right in the tree.
 * See detailed documentation about difficult cases below.
 * <pre>
 * If fromInclusive <= toExclusive, then seek forwards, otherwise seek backwards
 * </pre>
 * <p>
 * Implementation note: there are assumptions that keys are unique in the tree.
 * <p>
 * <strong>Note on backwards seek</strong>
 * <p>
 * To allow for lock free concurrency control the GBPTree agree to only move keys between nodes in 'forward' direction,
 * from left to right. This means when we do normal forward seek we never risk having keys moved 'passed' us and
 * therefore we can always be sure that we never miss any keys when traversing the leaf nodes or end up in the middle
 * of the range when traversing down the internal nodes. This assumption, of course, does not hold when seeking
 * backwards.
 * There are two complicated cases where we risk missing keys.
 * Case 1 - Split in current node
 * Case 2 - Split in next node while seeker is moving to that node
 * <p>
 * <strong>Case 1 - Split in current node</strong>
 * <p>
 * <em>Forward seek</em>
 * <p>
 * Here, the seeker is seeking forward and has read K0, K1, K2 and is about to read K3.
 * Then K4 is suddenly inserted and a split happens.
 * Seeker will now wake up and find that he is now outside the range of (previously returned key, end of range).
 * But he knows that he can continue to read forward until he hits the previously returned key, which is K2 and
 * then continue to return the next key, K3.
 * <pre>
 *     Seeker->
 *        v
 * [K0 K1 K2 K3]
 *
 *     Seeker->
 *        v
 * [K0 K1 __ __]<->[K2 K3 K4 __]
 * </pre>
 * <em>Backward seek</em>
 * <p>
 * Here, the seeking is seeking backwards and has only read K3 and is about to read K2.
 * Again, K4 is inserted, causing the same split as above.
 * Seeker now wakes up and find that the next key he can return is K1. What he do not see is that K2 has been
 * moved to the previous sibling and so, because he can not find the previously returned key, K3, in the current
 * node and because the next to return, K1, is located to the far right in the current node he need to jump back to
 * the previous sibling to find where to start again.
 * <pre>
 *      <-Seeker
 *           v
 * [K0 K1 K2 K3]
 *
 *      <-Seeker (WRONG)
 *           v
 * [K0 K1 __ __]<->[K2 K3 K4 __]
 *
 *                <-Seeker (RIGHT)
 *                     v
 * [K0 K1 __ __]<->[K2 K3 K4 __]
 * </pre>
 * <p>
 * <strong>Case 2 - Split in next node while seeker is moving to that node</strong>
 * <p>
 * <em>Forward seek</em>
 * <p>
 * Seeker has read K0, K1 on node 1  and has just moved to right sibling, node 2.
 * Now, K6 is inserted and a split  happens in node 2, right sibling of node 1.
 * Seeker wakes up and continues to read on node 2. Everything is fine.
 * <pre>
 *                   Seeker->
 *                      v
 * 1:[K0 K1 __ __]<->2:[K2 K3 K4 K5]
 *
 *                   Seeker->
 *                      v
 * 1:[K0 K1 __ __]<->2:[K2 K3 __ __]<->3:[K4 K5 K6 __]
 * </pre>
 * <em>Backward seek</em>
 * <p>
 * Seeker has read K4, K5 and has just moved to left sibling, node 1.
 * Insert K6, split in node 2. Note that keys are move to node 3, passed our seeker.
 * Seeker wakes up and see K1 as next key but misses K3 and K2.
 * <pre>
 *       <--Seeker
 *             v
 * 1:[K0 K1 K2 K3]<->2:[K4 K5 __ __]
 *
 *        <-Seeker
 *             v
 * 1:[K0 K1 __ __]<->3:[K2 K3 K4 __]<->2:[K5 K6 __ __]
 * </pre>
 * To guard for this, seeker 'scout' next sibling before moving there and read first key that he expect to see, K3
 * in this case. By using a linked cursor to 'scout' we create a consistent read over the node gap. If there is
 * suddenly another key when he goes there he knows that he could have missed some keys and he needs to go back until
 * he find the place where he left off, K4.
 */
class SeekCursor<KEY, VALUE> implements Seeker<KEY, VALUE> {
    interface Monitor {
        /**
         * @param depth where {@code depth==0} is the root.
         * @param keyCount number of keys in the visited internal node.
         */
        default void internalNode(int depth, int keyCount) {}

        /**
         * @param depth where {@code depth==0} is a root-only tree, where the root is a leaf.
         * @param keyCount number of keys in the visited leaf node.
         */
        default void leafNode(int depth, int keyCount) {}

        default void sibling() {}

        default void restartFromRoot() {}
    }

    static final Monitor NO_MONITOR = new Monitor() {};

    static final int DEFAULT_MAX_READ_AHEAD = 20;
    static final int LEAF_LEVEL = Integer.MAX_VALUE;

    private RootInitializer rootInitializer;

    /**
     * Cursor for reading from tree nodes and also will be moved around when following pointers.
     */
    private final PageCursor cursor;

    /**
     * underlying page cursor context
     */
    private final CursorContext cursorContext;

    private final Cache cache = new Cache();

    /**
     * Initially set to {@code false} after a {@link #readAndValidateNextKeyValueBatch(boolean)} ()} and will become {@code true}
     * as soon as coming across a key which is a result key. From that point on and until the next batch read,
     * having this a {@code true} will allow fewer comparisons to figure out whether or not a key is a result key.
     */
    private boolean resultOnTrack;

    /**
     * Provided when constructing the {@link SeekCursor}, marks the start (inclusive) of the key range to seek.
     * Comparison with {@link #toExclusive} decide if seeking forwards or backwards.
     */
    private KEY fromInclusive;

    /**
     * Provided when constructing the {@link SeekCursor}, marks the end (exclusive) of the key range to seek.
     * Comparison with {@link #fromInclusive} decide if seeking forwards or backwards.
     */
    private KEY toExclusive;

    /**
     * True if seeker is performing an exact match lookup, {@link #toExclusive} will then be treated as inclusive.
     */
    private boolean exactMatch;

    /**
     * {@link Layout} instance used to perform some functions around keys, like copying and comparing.
     */
    private final Layout<KEY, VALUE> layout;

    /**
     * Logic for reading data from leaf tree nodes.
     */
    private final LeafNodeBehaviour<KEY, VALUE> leafNode;

    /**
     * Logic for reading data from internal tree nodes.
     */
    private final InternalNodeBehaviour<KEY> internalNode;

    /**
     * Contains the highest returned key, i.e. from the last call to {@link #next()} returning {@code true}.
     */
    private final KEY prevKey;

    /**
     * Retrieves latest generation, only used when noticing that reading given a stale generation.
     */
    private final LongSupplier generationSupplier;

    /**
     * Retrieves latest root id and generation, moving the {@link PageCursor} to the root id and returning
     * the root generation. This is used when a query is re-traversing from the root, due to e.g. ending up
     * on a reused tree node and not knowing how to proceed from there.
     */
    private RootCatchup rootCatchup;

    /**
     * What level of the tree to search, {@link #LEAF_LEVEL} indicate always seek the leaves.
     */
    private int searchLevel;

    /**
     * Whether or not some result has been found, i.e. if {@code true} if there have been no call to
     * {@link #next()} returning {@code true}, otherwise {@code false}. If {@code false} then value in
     * {@link #prevKey} can be used and trusted.
     */
    private boolean first = true;

    /**
     * Current stable generation from this seek cursor's POV. Can be refreshed using {@link #generationSupplier}.
     */
    private long stableGeneration;

    /**
     * Current stable generation from this seek cursor's POV. Can be refreshed using {@link #generationSupplier}.
     */
    private long unstableGeneration;

    // *** Data structures for the current b-tree node ***

    /**
     * Position in current node, this is used when scanning the values of a leaf, each call to {@link #next()}
     * incrementing this position and reading the next key/value.
     */
    private int pos;

    /**
     * Number of keys in the current leaf, this value is cached and only re-read every time there's
     * a {@link PageCursor#shouldRetry() retry due to concurrent write}.
     */
    private int keyCount;

    /**
     * {@link TreeNodeUtil#generation(PageCursor) generation} of the current leaf node, read every call to {@link #next()}.
     */
    private long currentNodeGeneration;

    /**
     * Generation of the pointer which was last followed, either a
     * {@link TreeNodeUtil#rightSibling(PageCursor, long, long) sibling} during scan or otherwise following
     * {@link TreeNodeUtil#successor(PageCursor, long, long) successor} or
     * {@link InternalNodeBehaviour#childAt(PageCursor, int, long, long) child}.
     */
    private long lastFollowedPointerGeneration;

    /**
     * Cached {@link TreeNodeUtil#generation(PageCursor) generation} of the current leaf node, read every time a pointer
     * is followed to a new node. Used to ensure that a node hasn't been reused between two calls to {@link #next()}.
     */
    private long expectedCurrentNodeGeneration;

    /**
     * Decide if seeker is configured to seek forwards or backwards.
     * <p>
     * {@code true} if {@code layout.compare(fromInclusive, toExclusive) <= 0}, otherwise false.
     */
    private boolean seekForward;

    /**
     * Add to {@link #pos} to move this {@code SeekCursor} forward in the seek direction.
     */
    private int stride;

    /**
     * Set within should retry loop.
     * <p>
     * Is node a {@link TreeNodeUtil#NODE_TYPE_TREE_NODE} or something else?
     */
    private byte nodeType;
    /**
     * Set within should retry loop.
     * <p>
     * Pointer to successor of node.
     */
    private PointerWithGeneration successor = PointerWithGeneration.EMPTY;

    /**
     * Set within should retry loop.
     * <p>
     * Is node internal or leaf?
     */
    private boolean isInternal;

    /**
     * Set within should retry loop.
     * <p>
     * Used to store next sibling pointer to follow if traversing along the leaves.
     */
    private PointerWithGeneration nextSibling;

    // ┌── Special variables for backwards seek ──┐
    // v                                          v

    /**
     * Set within should retry loop.
     * <p>
     * Pointer to sibling opposite to seek direction. Only used when seeking backwards.
     */
    private PointerWithGeneration prevSibling;

    /**
     * Set by linked cursor scouting next sibling to go to when seeking backwards.
     * If first key when reading from next sibling node is not equal to this we
     * may have missed some keys that was moved passed us and we need to start
     * over from previous node.
     */
    private final KEY expectedFirstAfterGoToNext;

    /**
     * Key on pos 0 if traversing forward, pos {@code keyCount - 1} if traversing backwards.
     * To be compared with {@link #expectedFirstAfterGoToNext}.
     */
    private final KEY firstKeyInNode;

    /**
     * {@code true} to indicate that first key in node needs to be verified to ensure no keys
     * was moved passed us while we where changing nodes.
     */
    private boolean verifyExpectedFirstAfterGoToNext;

    /**
     * Whether or not this seeker has come to the end of its result set and will not attempt to continue.
     * This will be reset to {@code false} in each {@link #initialize(RootInitializer, RootCatchup, Object, Object, int, int, Monitor)}.
     */
    private boolean ended;

    /**
     * Whether or not this seeker has been closed.
     * This will be set to {@code true} in {@link #close()} and cannot be reset to {@code false} after that point.
     */
    private boolean closed;

    /**
     * Decorator for caught exceptions, adding information about which tree the exception relates to.
     */
    private final Consumer<Throwable> exceptionDecorator;

    /**
     * Monitor for internal seek events.
     */
    private Monitor monitor;

    /**
     * Normally {@link #readHeader()} is called when concurrent write happened. However, this flag
     * guards for cases where the header must be read despite no concurrent writes
     * such as when moving over to the next sibling and continuing reading.
     */
    private boolean forceReadHeader;

    private final int maxKeyCount;

    /**
     * Temporary key instance to use in key searches
     */
    private final KEY tempKey;

    SeekCursor(
            PageCursor cursor,
            Layout<KEY, VALUE> layout,
            LeafNodeBehaviour<KEY, VALUE> leafNode,
            InternalNodeBehaviour<KEY> internalNode,
            LongSupplier generationSupplier,
            Consumer<Throwable> exceptionDecorator,
            CursorContext cursorContext) {
        this.cursor = cursor;
        this.cursorContext = cursorContext;
        this.layout = layout;
        this.exceptionDecorator = exceptionDecorator;
        this.generationSupplier = generationSupplier;
        this.leafNode = leafNode;
        this.internalNode = internalNode;
        this.prevKey = layout.newKey();
        this.expectedFirstAfterGoToNext = layout.newKey();
        this.firstKeyInNode = layout.newKey();
        this.maxKeyCount = Math.max(leafNode.maxKeyCount(), internalNode.maxKeyCount());
        this.tempKey = layout.newKey();
    }

    SeekCursor<KEY, VALUE> initialize(
            RootInitializer rootInitializer,
            RootCatchup rootCatchup,
            KEY fromInclusive,
            KEY toExclusive,
            int maxReadAhead,
            int searchLevel,
            Monitor monitor)
            throws IOException {
        Preconditions.checkState(!closed, "Seeker already closed");
        this.rootInitializer = rootInitializer;
        this.rootCatchup = rootCatchup;
        this.lastFollowedPointerGeneration = rootInitializer.goToRoot(cursor, cursorContext);
        long generation = generationSupplier.getAsLong();
        this.stableGeneration = Generation.stableGeneration(generation);
        this.unstableGeneration = Generation.unstableGeneration(generation);
        this.resultOnTrack = false;
        this.expectedCurrentNodeGeneration = 0;
        this.verifyExpectedFirstAfterGoToNext = false;
        this.forceReadHeader = false;
        this.fromInclusive = fromInclusive;
        this.toExclusive = toExclusive;
        this.exactMatch = layout.compare(fromInclusive, toExclusive) == 0;
        this.first = true;
        this.seekForward = layout.compare(fromInclusive, toExclusive) <= 0;
        this.stride = seekForward ? 1 : -1;
        this.searchLevel = searchLevel;
        this.monitor = monitor;
        this.cache.init(exactMatch ? 1 : maxReadAhead);
        this.ended = false;
        this.pos = 0;
        this.keyCount = 0;
        this.currentNodeGeneration = 0;
        this.nodeType = 0;
        this.successor = PointerWithGeneration.EMPTY;
        this.isInternal = false;
        this.nextSibling = PointerWithGeneration.EMPTY;
        this.prevSibling = PointerWithGeneration.EMPTY;
        return this;
    }

    @VisibleForTesting
    SeekCursor<KEY, VALUE> withMonitor(Monitor monitor) {
        this.monitor = monitor;
        return this;
    }

    /**
     * Traverses from the root down to the node on target level (usually leaf) containing the next key that we're looking for,
     * or the first one provided in the constructor if no result have yet been returned.
     * <p>
     * This method is called when constructing the cursor, but also if leaf scan
     * later on ends up on an unexpected tree node (typically due to concurrent changes,
     * checkpoint and tree node reuse).
     * <p>
     * Before calling this method the caller is expected to place the {@link PageCursor} at the root, by using
     * {@link #rootCatchup}. After this method returns, the {@link PageCursor} is placed on the node on target level
     * (usually leaf) containing the next result and {@link #pos} is also initialized correctly.
     *
     * @throws IOException on {@link PageCursor} error.
     */
    private void traverseDownToCorrectLevel() throws IOException {
        traverseToTargetLevel(searchLevel);
        cache.clear();
    }

    private void traverseToTargetLevel(int searchLevel) throws IOException {
        int currentReadLevel = 0;
        while (true) {
            try {
                var childPointer = readHeaderAndSearchForChild();
                // some checks
                if (endedUpOnUnWrongNode()) {
                    throw new TreeNodeOutOfBoundsException(cursor.getCurrentPageId(), "ended up on wrong node");
                }
                sanityCheck();
                if (goToSuccessor()) {
                    continue;
                }
                monitorReadLevel(isInternal, currentReadLevel, keyCount);

                if (!isInternal || currentReadLevel == searchLevel) {
                    // reached bottom or target level
                    break;
                }

                // move down
                goTo(childPointer.pointer(), childPointer.generation(), GBPPointerType.CHILD, false);
                currentReadLevel++;
            } catch (TreeNodeOutOfBoundsException e) {
                prepareToStartFromRoot();
                isInternal = true;
                currentReadLevel = 0;
            }
        }
    }

    private void monitorReadLevel(boolean internal, int readLevel, int keyCount) {
        if (internal) {
            monitor.internalNode(readLevel, keyCount);
        } else {
            monitor.leafNode(readLevel, keyCount);
        }
    }

    private void sanityCheck() {
        if (!keyCountIsSane(keyCount)) {
            throw new TreeInconsistencyException(
                    "Read inconsistent tree node %d%n"
                            + "  nodeType:%d%n  currentNodeGeneration:%d%n  successor:%d%n  successorGeneration:%d%n"
                            + "  isInternal:%b%n  keyCount:%d%n  pos:%d%n"
                            + "  childId:%d%n  childIdGeneration:%d",
                    cursor.getCurrentPageId(),
                    nodeType,
                    currentNodeGeneration,
                    successor.pointer(),
                    successor.generation(),
                    isInternal,
                    keyCount,
                    pos,
                    nextSibling.pointer(),
                    nextSibling.generation());
        }
    }

    private PointerWithGeneration readHeaderAndSearchForChild() throws IOException {
        PointerWithGeneration result = null;
        do {
            try {
                if (!readHeader()) {
                    continue;
                }
                if (isInternal) {
                    int searchResult =
                            KeySearch.search(cursor, internalNode, fromInclusive, tempKey, keyCount, cursorContext);
                    int childPos = KeySearch.childPositionOf(searchResult);
                    result = internalNode.childWithGenerationAt(cursor, childPos, stableGeneration, unstableGeneration);
                }
            } catch (Exception e) {
                cursor.setCursorException(e.getMessage());
            }
        } while (cursor.shouldRetry());
        checkOutOfBounds(cursor);
        cursor.checkAndClearCursorException();
        return result;
    }

    private boolean advance() throws IOException {
        initialTraverseDownIfNeeded();

        // on the first call to next we set concurrentWriteHappened flag to true in order to trigger binary search
        // in the leaf on subsequent calls first will be false, or ended will be true
        boolean concurrentWriteHappened = first;
        while (!ended) {
            pos += stride;

            // There are two main tracks in this loop:
            // - (SLOW) no keys/values have been read and will therefore need to be read from the cursor.
            //   Reading from the cursor means there are a lot of things around the actual keys and values
            //   that need to be check to validate the read. This is expensive to do since there's so much
            //   to validate. This is why keys/values are read in batches of N entries. The validations
            //   are made only once per batch instead of once per key/value.
            // - (FAST) there are keys/values read and validated and ready to simply be returned to the user.

            if (cache.hasNext()) {
                // FAST, key/value is readily available
                concurrentWriteHappened = cursor.shouldRetry();
                if (!concurrentWriteHappened) {
                    cache.next();
                    if (resultOnTrack && isValueVisible(cache.currentValue())) {
                        return true;
                    }
                    if (insidePrevKey(cache.currentKey())) {
                        first = false;
                        resultOnTrack = true;
                        if (isValueVisible(cache.currentValue())) {
                            return true;
                        }
                    } else {
                        // key is before prevKey or fromIncluded which happens when we read through leaf split when
                        // following sibling pointer. this is fine
                        if (first) {
                            // preserve old logic setting concurrentWriteHappened when haven't seen the first valid
                            // key yet
                            // though it is overwritten on the next iteration _if cache is not empty_
                            // it means that if we haven't seen the first element yet and read full cache of values
                            // that are before fromInclude (due to splits) we check all those cache entries first,
                            // and only after that trigger binary search
                            concurrentWriteHappened = true;
                        }
                    }
                    continue;
                }
            }
            // SLOW, next batch of keys/values needs to be read
            if (resultOnTrack) {
                layout.copyKey(cache.currentKey(), prevKey);
            }
            concurrentWriteHappened = !readAndValidateNextKeyValueBatch(concurrentWriteHappened);
            if (concurrentWriteHappened) {
                // Concurrent changes
                cache.clear();
                continue;
            }

            if (!seekForward && pos >= keyCount) {
                if (goTo(prevSibling.pointer(), prevSibling.generation(), GBPPointerType.RIGHT_SIBLING, true)) {
                    concurrentWriteHappened = true;
                }
                // Continue in the read loop above so that we can continue reading from previous sibling
                // or on next position
                continue;
            }

            if (seekForward && pos >= keyCount || !seekForward && pos < 0) {
                switch (goToNextSibling()) {
                    case STOP -> {}
                    case RETRY_READ -> {
                        concurrentWriteHappened = true;
                        continue;
                    }
                    case FOLLOW -> {
                        continue;
                    }
                }
            }
            // at this point we identified pos to return and probably filled read-ahead cache from that position
            // next iteration should return entry at that pos using cache, so move pos back to prepare for it
            pos -= stride;

            // wasn't able to fill cache or move to sibling, that's it
            ended = cache.empty();
        }
        return false;
    }

    @Override
    public boolean next() throws IOException {
        try {
            while (true) {
                try {
                    return advance();
                } catch (TreeNodeOutOfBoundsException e) {
                    prepareToStartFromRoot();
                    traverseDownToCorrectLevel();
                }
            }
        } catch (Throwable e) {
            exceptionDecorator.accept(e);
            throw e;
        }
    }

    private void initialTraverseDownIfNeeded() throws IOException {
        if (first && !ended) {
            traverseDownToCorrectLevel();
        }
    }

    private boolean readAndValidateNextKeyValueBatch(boolean writeHappened) throws IOException {
        boolean retry = writeHappened;
        do {
            try {
                cache.clear();
                resultOnTrack = false;

                if (retry || forceReadHeader || !seekForward) {
                    if (!readHeader() || (isInternal && searchLevel == LEAF_LEVEL)) {
                        retry = true;
                        continue;
                    }
                }

                if (verifyExpectedFirstAfterGoToNext) {
                    assert !seekForward;
                    pos = keyCount - 1;
                    (isInternal ? internalNode : leafNode).keyAt(cursor, firstKeyInNode, pos, cursorContext);
                }

                boolean fillCache = true;
                if (retry) {
                    // Keys could have been moved so we need to make sure we are not missing any keys by
                    // moving position back until we find previously returned key
                    KEY key = first ? fromInclusive : prevKey;

                    int searchResult = KeySearch.search(
                            cursor, isInternal ? internalNode : leafNode, key, tempKey, keyCount, cursorContext);

                    pos = selectPosition(searchResult, first, seekForward, keyCount);
                    if (exactMatch && !KeySearch.isHit(searchResult)) {
                        fillCache = false;
                    }
                }
                if (fillCache) {
                    cache.fill(pos);
                }
            } catch (Exception e) {
                String message =
                        e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
                cursor.setCursorException(message, e);
            }
            retry = true;
        } while (cursor.shouldRetry());
        checkOutOfBoundsAndClosed();
        cursor.checkAndClearCursorException();

        if (endedUpOnUnWrongNode() || (isInternal && searchLevel == LEAF_LEVEL)) {
            // This node has been reused for something else than a tree node. Restart seek from root.
            throw new TreeNodeOutOfBoundsException(cursor.getCurrentPageId(), "ended up on wrong node");
        }
        sanityCheck();

        if (!verifyFirstKeyInNodeIsExpectedAfterGoTo()) {
            return false;
        }

        //noinspection RedundantIfStatement
        if (goToSuccessor()) {
            return false;
        }

        return true;
    }

    /**
     * Binary search returns position of the element if it is hit, or position where that element would be inserted if no hit
     * This gives us different rules on selecting next position to return depending on direction of seek, or if hit should be included.
     * And there is special case when result equals keyCount.
     */
    private int selectPosition(int searchResult, boolean inclusive, boolean seekForward, int keyCount) {
        int position = KeySearch.positionOf(searchResult);
        boolean hit = KeySearch.isHit(searchResult);
        if (seekForward) {
            // found exact match but need to skip it
            if (hit && !inclusive) {
                return position + 1;
            }
            return position;
        }
        // when seeking backwards there are few options:
        // - no exact match - move left to get correct element
        // - exact match, but not inclusive - need to skip element
        // - special case, position == keyCount, this is no exact match, but we don't move left
        //      in this case we could've come here after traversal from root and there is no match in tree at all,
        //      or maybe there were concurrent updates since traversal and split/rebalance moved target element
        //      to the right sibling.
        //      we return keyCount here which triggers jump back to the right sibling, and subsequent move
        //      to the left sibling (this node) sets {@link #verifyExpectedFirstAfterGoToNext} which updates pos to
        //      "keyCount - 1"
        if (position == keyCount) {
            return position;
        }
        if (!hit || !inclusive) {
            return position - 1;
        }
        return position;
    }

    /**
     * Check out of bounds for cursor. If out of bounds, check if seeker has been closed and throw exception accordingly
     */
    private void checkOutOfBoundsAndClosed() {
        try {
            checkOutOfBounds(cursor);
        } catch (TreeInconsistencyException e) {
            // Only check the closed status here when we get an out of bounds to avoid making
            // this check for every call to next.
            Preconditions.checkState(!closed, "Tried to use seeker after it was closed");
            throw e;
        }
    }

    /**
     * @return whether or not the key is "before" the end of the key range
     * ({@link #toExclusive}) of this seek.
     */
    private boolean insideEndRange(boolean exactMatch, KEY key) {
        int compare = layout.compare(key, toExclusive);
        if (exactMatch) {
            return seekForward ? compare <= 0 : compare >= 0;
        } else {
            return seekForward ? compare < 0 : compare > 0;
        }
    }

    /**
     * @return whether or not the key is "after" the start of the key range
     * ({@link #fromInclusive}) of this seek.
     */
    private boolean insideStartRange(KEY key) {
        int compare = layout.compare(key, fromInclusive);
        return seekForward ? compare >= 0 : compare <= 0;
    }

    /**
     * @return whether or not the key is "after" the last returned key of this seek
     * ({@link #prevKey}), or if no result has been returned the start of the key range ({@link #fromInclusive}).
     */
    private boolean insidePrevKey(KEY key) {
        if (first) {
            return insideStartRange(key);
        }
        int compare = layout.compare(key, prevKey);
        return seekForward ? compare > 0 : compare < 0;
    }

    /**
     * Tries to move the {@link PageCursor} to the tree node specified inside {@code pointerId},
     * also setting the pointer generation expectation on the next read on that new tree node.
     * <p>
     * As with all pointers, the generation is checked for sanity and if generation looks to be in the future,
     * there's a generation catch-up made and the read will have to be re-attempted.
     *
     * @param pointerId read result containing pointer id to go to.
     * @param pointerGeneration generation of {@code pointerId}.
     * @param type type of pointer, e.g. "child" or "sibling" or so.
     * @return {@code true} if context was updated or {@link PageCursor} was moved, both cases meaning that
     * caller should retry its most recent read, otherwise {@code false} meaning that nothing happened.
     * @throws IOException on {@link PageCursor} error.
     * @throws TreeInconsistencyException if {@code allowNoNode} is {@code true} and {@code pointerId}
     * contains a "null" tree node id.
     */
    private boolean goTo(long pointerId, long pointerGeneration, String type, boolean allowNoNode) throws IOException {
        if (pointerCheckingWithGenerationCatchup(pointerId, allowNoNode, type)) {
            return true;
        } else if (!allowNoNode || TreeNodeUtil.isNode(pointerId)) {
            TreeNodeUtil.goTo(cursor, type, pointerId);
            lastFollowedPointerGeneration = pointerGeneration;
            return true;
        }
        return false;
    }

    /**
     * Calls {@link #goTo(long, long, String, boolean)} with successor fields.
     */
    private boolean goToSuccessor() throws IOException {
        return goTo(successor.pointer(), successor.generation(), GBPPointerType.SUCCESSOR, true);
    }

    /**
     * @return {@code false} if there was a set expectancy on first key in tree node which weren't met,
     * otherwise {@code true}. Caller should
     */
    private boolean verifyFirstKeyInNodeIsExpectedAfterGoTo() {
        boolean result = true;
        if (verifyExpectedFirstAfterGoToNext && layout.compare(firstKeyInNode, expectedFirstAfterGoToNext) != 0) {
            result = false;
        }
        verifyExpectedFirstAfterGoToNext = false;
        return result;
    }

    /**
     * @return the read previous sibling, depending on the direction this seek is going.
     */
    private PointerWithGeneration readPrevSibling() {
        return seekForward
                ? TreeNodeUtil.leftSibling(cursor, stableGeneration, unstableGeneration)
                : TreeNodeUtil.rightSibling(cursor, stableGeneration, unstableGeneration);
    }

    /**
     * @return the read next sibling, depending on the direction this seek is going.
     */
    private PointerWithGeneration readNextSibling() {
        return seekForward
                ? TreeNodeUtil.rightSibling(cursor, stableGeneration, unstableGeneration)
                : TreeNodeUtil.leftSibling(cursor, stableGeneration, unstableGeneration);
    }

    /**
     * @return {@code true} if header was read and looks sane, otherwise {@code false} meaning that node doesn't look
     * like a tree node or we can expect a shouldRetry to take place.
     */
    private boolean readHeader() {
        nodeType = TreeNodeUtil.nodeType(cursor);
        if (nodeType != TreeNodeUtil.NODE_TYPE_TREE_NODE) {
            // If this node doesn't even look like a tree node then anything we read from it
            // will be just random data when looking at it as if it were a tree node.
            return false;
        }

        isInternal = TreeNodeUtil.isInternal(cursor);
        keyCount = TreeNodeUtil.keyCount(cursor);
        currentNodeGeneration = TreeNodeUtil.generation(cursor);
        successor = TreeNodeUtil.successor(cursor, stableGeneration, unstableGeneration);
        prevSibling = readPrevSibling();
        nextSibling = readNextSibling();

        forceReadHeader = false;
        return keyCountIsSane(keyCount);
    }

    private boolean endedUpOnUnWrongNode() {
        return nodeType != TreeNodeUtil.NODE_TYPE_TREE_NODE || !verifyNodeGenerationInvariants();
    }

    private enum NextSiblingResult {
        STOP,
        FOLLOW,
        RETRY_READ
    }

    /**
     * Moves {@link PageCursor} to next sibling (read before this call into {@link #nextSibling}).
     * Also, on backwards seek, calls {@link #scoutNextSibling()} to be able to verify consistent read on
     * new sibling even on concurrent writes.
     * <p>
     * As with all pointers, the generation is checked for sanity and if generation looks to be in the future,
     * there's a generation catch-up made and the read will have to be re-attempted.
     *
     * @return {@code true} if we should read more after this call, otherwise {@code false} to mark the end.
     * @throws IOException on {@link PageCursor} error.
     */
    private NextSiblingResult goToNextSibling() throws IOException {
        if (pointerCheckingWithGenerationCatchup(
                nextSibling.pointer(),
                true,
                seekForward ? GBPPointerType.RIGHT_SIBLING : GBPPointerType.LEFT_SIBLING)) {
            // Reading sibling pointer resulted in a bad read, but generation had changed
            // (a checkpoint has occurred since we started this cursor) so the generation fields in this
            // cursor are now updated with the latest, so let's try that read again.
            return NextSiblingResult.RETRY_READ;
        } else if (TreeNodeUtil.isNode(nextSibling.pointer())) {
            monitor.sibling();
            var result = NextSiblingResult.FOLLOW;
            if (seekForward) {
                // TODO: Check if rightSibling is within expected range before calling next.
                // TODO: Possibly by getting highest expected from IdProvider
                TreeNodeUtil.goTo(cursor, "sibling", nextSibling.pointer());
                lastFollowedPointerGeneration = nextSibling.generation();
                if (first) {
                    // Have not yet found first hit among leaves.
                    // First hit can be several leaves to the right.
                    // Continue to use binary search in right leaf
                    result = NextSiblingResult.RETRY_READ;
                } else {
                    // It is likely that first key in right sibling is a next hit.
                    // Continue using scan
                    forceReadHeader = true;
                    pos = -1;
                }
            } else {
                // Need to scout next sibling because we are seeking backwards
                if (scoutNextSibling()) {
                    TreeNodeUtil.goTo(cursor, "sibling", nextSibling.pointer());
                    verifyExpectedFirstAfterGoToNext = true;
                    lastFollowedPointerGeneration = nextSibling.generation();
                } else {
                    result = NextSiblingResult.RETRY_READ;
                }
            }
            return result;
        }

        // The current node is exhausted and it had no sibling to read more from.
        return NextSiblingResult.STOP;
    }

    /**
     * Reads first key on next sibling, without moving the main {@link PageCursor} to that sibling.
     * This to be able to guard for, and retry read if, concurrent writes moving keys in the "wrong" direction.
     * The first key read here will be matched after actually moving the main {@link PageCursor} to
     * the next sibling.
     * <p>
     * May only be called if {@link #nextSibling} points to next sibling.
     *
     * @return {@code true} if first key in next sibling was read successfully, otherwise {@code false},
     * which means that caller should retry most recent read.
     * @throws IOException on {@link PageCursor} error.
     */
    private boolean scoutNextSibling() throws IOException {
        assert !seekForward; // only happens when we go backward
        assert !isInternal; // only happens when we go through leafs

        try (PageCursor scout =
                this.cursor.openLinkedCursor(GenerationSafePointerPair.pointer(nextSibling.pointer()))) {
            scout.next();

            if (TreeNodeUtil.nodeType(scout) != TreeNodeUtil.NODE_TYPE_TREE_NODE) {
                // If for any reason the node we're scouting is not a tree node anymore
                // we return false to signal to the caller that it needs to restart the
                // procedure to find the next sibling going backwards.
                return false;
            }
            if (!TreeNodeUtil.isLeaf(scout)) {
                // If for any reason the node we're scouting is not a leaf node as it should,
                // possibly because a race condition, we return false to signal to the caller
                // that it needs to restart the procedure to find the next sibling going backwards.
                return false;
            }

            int keyCount = TreeNodeUtil.keyCount(scout);
            if (keyCount <= 0 || keyCount > maxKeyCount) {
                // If the keyCount of the node we're scouting is 0 (so no keys there) or if there
                // are more keys that what should be possible, then we're observing some
                // intermediate state of computation or something else went wrong.
                // We return false to signal to the caller that is should restart the procedure
                // to find the next sibling going backwards.
                return false;
            }

            // Our next entry going backwards is the last key on the previous leaf node.
            int firstPosInBackwardsSibling = keyCount - 1;
            leafNode.keyAt(scout, expectedFirstAfterGoToNext, firstPosInBackwardsSibling, cursorContext);

            if (this.cursor.shouldRetry()) {
                // We scouted next sibling but either next sibling or current node has been changed
                // since we left shouldRetry loop, this means keys could have been moved passed us
                // and we need to start over.
                // Because we also need to restart read on current node there is no use to loop
                // on shouldRetry here.
                return false;
            }
            checkOutOfBounds(this.cursor);
        }
        // If we reached here, we passed all the checks while scouting the next node and we have saved
        // what should be the key for the next entry in expectedFirstAfterGoToNext. We return true so the
        // caller knows we're good to proceed in the backwards scan.
        return true;
    }

    /**
     * @return true if key-value pair at current cachedIndex should be visible by users of the seeker.
     * When at internal node, there are no values and all keys should be visible.
     */
    private boolean isValueVisible(ValueHolder<VALUE> value) {
        return isInternal || !value.deleted;
    }

    /**
     * {@link TreeNodeUtil#keyCount(PageCursor) keyCount} is the only value read inside a do-shouldRetry loop
     * which is used as data fed into another read. Because of that extra assertions are made around
     * keyCount, both inside do-shouldRetry (requesting one more round in the loop) and outside
     * (calling this method, which may throw exception).
     *
     * @param keyCount key count of a tree node.
     * @return {@code true} if key count is sane, i.e. positive and within max expected key count on a tree node.
     */
    private boolean keyCountIsSane(int keyCount) {
        // if keyCount is out of bounds of what a tree node can hold, it must be that we're
        // reading from an evicted page that just happened to look like a tree node.
        return keyCount >= 0 && keyCount <= maxKeyCount;
    }

    /**
     * Perform a generation catchup, updates current root and update range to start from
     * previously returned key. Should be followed by a call to {@link #traverseDownToCorrectLevel()}
     * or if already in that method just loop again.
     * <p>
     * Caller should retry most recent read after calling this method.
     *
     * @throws IOException on {@link PageCursor}.
     */
    private void prepareToStartFromRoot() throws IOException {
        while (true) {
            try {
                monitor.restartFromRoot();
                generationCatchup();
                Root root = rootCatchup.catchupFrom(cursor.getCurrentPageId(), cursorContext);
                lastFollowedPointerGeneration = root.goTo(cursor);
                if (!first) {
                    fromInclusive = layout.copyKey(prevKey);
                }

                // Reset mutable state
                cache.clear();
                resultOnTrack = false;
                pos = 0;
                keyCount = 0;
                verifyExpectedFirstAfterGoToNext = false;
                currentNodeGeneration = 0;
                expectedCurrentNodeGeneration = 0;
                nodeType = 0;
                successor = PointerWithGeneration.EMPTY;
                isInternal = false;
                nextSibling = PointerWithGeneration.EMPTY;
                prevSibling = PointerWithGeneration.EMPTY;
                forceReadHeader = false;
                break;
            } catch (TreeNodeOutOfBoundsException e) {
                // If benign: in between getting the root and navigating to it there was a compaction
                //            and the root moved to a lower region of the file
                // If malignant: there's a pointer in the tree that points to a tree node that is outside
                //               the boundaries of the file. If this happens, the rootCatchup has a trip counter
                //               which will prevent an endless loop here.
            }
        }
    }

    /**
     * Verifies that the generation of the tree node arrived at matches the generation of the pointer
     * pointing to the tree node. Generation of the node cannot be higher than the generation of the pointer -
     * if it is then it means that the tree node has been removed (or made obsolete) and reused since we read the
     * pointer pointing to it and that the seek is now in an invalid location and needs to be restarted from the root.
     *
     * @return {@code true} if generation matches, otherwise {@code false} if seek needs to be restarted from root.
     */
    private boolean verifyNodeGenerationInvariants() {
        if (lastFollowedPointerGeneration != 0) {
            if (currentNodeGeneration > lastFollowedPointerGeneration) {
                // We've just followed a pointer to a new node, we have arrived there and made
                // the first read on it. It looks like the node we arrived at have a higher generation
                // than the pointer generation, this means that this node node have been reused between
                // following the pointer and reading the node after getting there.
                return false;
            }
            lastFollowedPointerGeneration = 0;
            expectedCurrentNodeGeneration = currentNodeGeneration;
        } else //noinspection RedundantIfStatement
        if (currentNodeGeneration != expectedCurrentNodeGeneration) {
            // We've read more than once from this node and between reads the node generation has changed.
            // This means the node has been reused.
            return false;
        }
        return true;
    }

    /**
     * Checks the provided pointer read and if not successful performs a generation catch-up with
     * {@link #generationSupplier} to allow reading that same pointer again given the updated generation context.
     *
     * @param pointer read result to check for success.
     * @param allowNoNode whether or not pointer is allowed to be "null".
     * @param pointerType string describing type of pointer that was read.
     * @return {@code true} if there was a generation catch-up called and generation was actually updated,
     * this means that caller should retry its most recent read.
     */
    private boolean pointerCheckingWithGenerationCatchup(long pointer, boolean allowNoNode, String pointerType) {
        if (!GenerationSafePointerPair.isSuccess(pointer)) {
            // An unexpected sibling read, this could have been caused by a concurrent checkpoint
            // where generation has been incremented. Re-read generation and, if changed since this
            // seek started then update generation locally
            if (generationCatchup()) {
                return true;
            }
            PointerChecking.checkPointer(
                    pointer, allowNoNode, cursor.getCurrentPageId(), pointerType, stableGeneration, unstableGeneration);
        }
        return false;
    }

    /**
     * Updates generation using the {@link #generationSupplier}. If there has been a generation change
     * since construction of this seeker or since last calling this method the generation context in this
     * seeker is updated.
     *
     * @return {@code true} if generation was updated, which means that caller should retry its most recent read.
     */
    private boolean generationCatchup() {
        long newGeneration = generationSupplier.getAsLong();
        long newStableGeneration = Generation.stableGeneration(newGeneration);
        long newUnstableGeneration = Generation.unstableGeneration(newGeneration);
        if (newStableGeneration != stableGeneration || newUnstableGeneration != unstableGeneration) {
            stableGeneration = newStableGeneration;
            unstableGeneration = newUnstableGeneration;
            return true;
        }
        return false;
    }

    /**
     * @return the key found from the most recent call to {@link #next()} returning {@code true}.
     * @throws IllegalStateException if no {@link #next()} call which returned {@code true} has been made yet.
     */
    @Override
    public KEY key() {
        assertHasResult();
        return cache.currentKey();
    }

    /**
     * @return the value found from the most recent call to {@link #next()} returning {@code true}.
     * @throws IllegalStateException if no {@link #next()} call which returned {@code true} has been made yet.
     */
    @Override
    public VALUE value() {
        assertHasResult();
        var value = cache.currentValue();
        assert !value.deleted;
        return value.value;
    }

    @Override
    public void close() {
        if (!closed) {
            cursor.close();
            closed = true;
            ended = true;
        }
    }

    private void assertHasResult() {
        if (first) {
            throw new IllegalStateException("There has been no successful call to next() yet");
        }
        if (closed) {
            throw new IllegalStateException("This cursor is closed");
        }
    }

    @Override
    public void reinitializeToNewRange(KEY fromInclusive, KEY toExclusive) {
        if (!ended) {
            try {
                //noinspection resource
                initialize(
                        rootInitializer,
                        rootCatchup,
                        fromInclusive,
                        this.toExclusive,
                        cache.size(),
                        searchLevel,
                        monitor);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private class Cache {
        private KEY[] keys;
        private ValueHolder<VALUE>[] values;
        private int index;
        private int length;

        @SuppressWarnings("unchecked")
        private void init(int batchSize) {
            if (keys == null || batchSize > keys.length) {
                keys = (KEY[]) new Object[batchSize];
                values = new ValueHolder[batchSize];
            }
            clear();
        }

        private void fill(int fromPos) throws IOException {
            int cacheIndex = 0;
            for (int readPos = fromPos;
                    cacheIndex < keys.length && 0 <= readPos && readPos < keyCount;
                    readPos += stride) {
                // Read the next value in this leaf
                initSlot(cacheIndex);
                readIntoSlot(cacheIndex, readPos);

                if (!insideEndRange(exactMatch, keys[cacheIndex])) {
                    // OK so we read too far, abort this ahead-reading
                    break;
                }
                cacheIndex++;
            }
            this.length = cacheIndex;
        }

        private void readIntoSlot(int index, int readPos) throws IOException {
            if (isInternal) {
                internalNode.keyAt(cursor, keys[index], readPos, cursorContext);
            } else {
                leafNode.keyValueAt(cursor, keys[index], values[index], readPos, cursorContext);
            }
        }

        private void initSlot(int cacheIndex) {
            if (keys[cacheIndex] == null) {
                keys[cacheIndex] = layout.newKey();
                values[cacheIndex] = new ValueHolder<>(layout.newValue());
            }
        }

        private ValueHolder<VALUE> currentValue() {
            return values[index];
        }

        private KEY currentKey() {
            return keys[index];
        }

        private boolean hasNext() {
            return index + 1 < length;
        }

        private void next() {
            index++;
        }

        private void clear() {
            index = -1;
            length = 0;
        }

        private int size() {
            return keys.length;
        }

        private boolean empty() {
            return length == 0;
        }
    }
}
