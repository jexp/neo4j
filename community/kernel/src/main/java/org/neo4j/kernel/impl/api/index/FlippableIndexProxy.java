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
package org.neo4j.kernel.impl.api.index;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.async.AsyncBlockAccessor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.kernel.api.exceptions.index.ExceptionDuringFlipKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexProxyAlreadyClosedKernelException;
import org.neo4j.kernel.api.exceptions.schema.IncompleteConstraintValidationException;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.TokenIndexReader;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.api.index.updater.DelegatingIndexUpdater;
import org.neo4j.values.storable.Value;

public class FlippableIndexProxy extends AbstractDelegatingIndexProxy {
    private volatile boolean closed;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
    private volatile IndexProxyFactory flipTarget;
    // This variable below is volatile because it can be changed in flip or flipTo
    // and even though it may look like acquiring the read lock, when using this variable
    // for various things, execution flow would go through a memory barrier of some sort.
    // But it turns out that that may not be the case. F.ex. ReentrantReadWriteLock
    // code uses unsafe compareAndSwap that sort of circumvents an equivalent of a volatile read.
    private volatile IndexProxy delegate;
    private boolean started;

    public FlippableIndexProxy() {
        this(null);
    }

    FlippableIndexProxy(IndexProxy originalDelegate) {
        this.delegate = originalDelegate;
    }

    @Override
    public IndexProxy getDelegate() {
        return delegate;
    }

    @Override
    public void start() {
        lock.readLock().lock();
        try {
            delegate.start();
            started = true;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public IndexUpdater newUpdater(IndexUpdateMode mode, CursorContext cursorContext, boolean parallel) {
        // Making use of reentrant locks to ensure that the delegate's constructor is called under lock protection
        // while still retaining the lock until a call to close on the returned IndexUpdater
        lock.readLock().lock();
        try {
            return new LockingIndexUpdater(delegate.newUpdater(mode, cursorContext, parallel));
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void drop() {
        lock.readLock().lock();
        try {
            closed = true;
            delegate.drop();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * The {@code force()}-method is called during log rotation. At this time we do not want to wait for locks held by
     * {@link LockingIndexUpdater}. Waiting on such locks would cause a serious risk of deadlocks, since very likely
     * the reader we would be waiting on would be waiting on the log rotation lock held by the thread calling this
     * method. The reason we would wait for a read lock while trying to acquire a read lock is if there is a third
     * thread waiting on the write lock, probably an index populator wanting to
     * {@linkplain #flip(Callable) flip the index into active state}.
     * <p/>
     * We avoid this deadlock situation by "barging" on the read lock, i.e. acquire it in an <i>unfair</i> way, where
     * we don't care about waiting threads, only about whether the exclusive lock is held or not.
     */
    @Override
    public void force(FileFlushEvent flushEvent, AsyncBlockAccessor asyncBlockAccessor, CursorContext cursorContext)
            throws IOException {
        barge(lock.readLock()); // see javadoc of this method (above) for rationale on why we use barge(...) here
        try {
            delegate.force(flushEvent, asyncBlockAccessor, cursorContext);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public long compact(FileFlushEvent flushEvent, AsyncBlockAccessor asyncBlockAccessor, CursorContext cursorContext)
            throws IOException {
        barge(lock.readLock()); // see javadoc of this method (above) for rationale on why we use barge(...) here
        try {
            return delegate.compact(flushEvent, asyncBlockAccessor, cursorContext);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void refresh() throws IOException {
        lock.readLock().lock();
        try {
            delegate.refresh();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Acquire the {@code ReadLock} in an <i>unfair</i> way, without waiting for queued up writers.
     * <p/>
     * The {@link ReentrantReadWriteLock.ReadLock#tryLock() tryLock}-method of the {@code ReadLock} implementation of
     * {@code ReentrantReadWriteLock} implements a <i>barging</i> behaviour, where if an exclusive lock is not held,
     * the shared lock will be acquired, even if there are other threads waiting for the lock. This behaviour is
     * regardless of whether the lock is fair or not.
     * <p/>
     * This allows us to avoid deadlocks where readers would wait for writers that wait for readers in critical
     * methods.
     * <p/>
     * The naive way to implement this method would be:
     * <pre><code>
     *     if ( !lock.tryLock() ) // try to barge
     *         lock.lock(); // fall back to normal blocking lock call
     * </code></pre>
     * This would however not implement the appropriate barging behaviour in a scenario like the following: Say the
     * exclusive lock is held, and there is a queue waiting containing first a reader and then a writer, in this case
     * the {@code tryLock()} method will return false. If the writer then finishes between the naive implementation
     * exiting {@code tryLock()} and before entering {@code lock()} the {@code barge(...)} method would now block in
     * the exact way we don't want it to block, with a read lock held and a writer waiting.<br/>
     * In order to get around this situation, the implementation of this method uses a
     * {@linkplain Lock#tryLock(long, TimeUnit) timed wait} in a retry-loop in order to ensure that we make another
     * attempt to barge the lock at a later point.
     * <p/>
     * This method is written to be compatible with the signature of {@link Lock#lock()}, which is not interruptible,
     * but implemented based on the interruptible {@link Lock#tryLock(long, TimeUnit)}, so the implementation needs to
     * remember being interrupted, and reset the flag before exiting, so that later invocations of interruptible
     * methods detect the interruption.
     *
     * @param lock a {@link java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock}
     */
    private static void barge(ReentrantReadWriteLock.ReadLock lock) {
        boolean interrupted = false;
        // exponential retry back-off, no more than 1 second
        for (long timeout = 10; !lock.tryLock(); timeout = Math.min(1000, timeout * 2)) {
            try {
                if (lock.tryLock(timeout, TimeUnit.MILLISECONDS)) {
                    return;
                }
            }
            // the barge()-method is uninterruptable, but implemented based on the interruptible tryLock()-method
            catch (InterruptedException e) {
                Thread.interrupted(); // ensure the interrupt flag is cleared
                interrupted = true; // remember to set interrupt flag before we exit
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt(); // reset the interrupt flag
        }
    }

    /**
     * Read the current {@link #delegate} without taking any lock, for the purpose of <i>observing</i> the index rather
     * than operating on it.
     * <p/>
     * {@link #flip(Callable)} holds the exclusive lock for as long as its action takes, and that action can be
     * arbitrarily long-lived: it closes the populator, which for vector indexes performs post-population segment
     * compaction that can run for many minutes on a large index. Acquiring the shared lock in order to merely report
     * index state would therefore block for that entire duration. That is not a theoretical concern - it made
     * {@code SHOW INDEXES} hang until compaction finished, and because the shared lock is acquired uninterruptibly the
     * blocked queries could not even be terminated, so they accumulated until the Bolt worker pool was exhausted and
     * unrelated queries started failing too.
     * <p/>
     * Doing this without the lock is safe because {@link #delegate} is {@code volatile}, so we always observe a fully
     * published proxy - either the one from before the flip or the one from after it, both of which are valid answers
     * for an observer - and because the methods routed through here only report status:
     * {@link IndexProxy#getDescriptor()} returns an immutable descriptor held in a {@code final} field;
     * {@link IndexProxy#getState()} is a constant everywhere except {@link TentativeConstraintIndexProxy}, which
     * derives it from a {@link java.util.concurrent.CopyOnWriteArrayList}; {@link IndexProxy#getPopulationFailure()}
     * throws, returns a {@code final} field, or - again in {@link TentativeConstraintIndexProxy} - reduces over that
     * same concurrent collection; and {@link IndexProxy#getIndexPopulationProgress()} does reach into the populator
     * that the flip is closing, but every {@link org.neo4j.kernel.api.index.IndexPopulator#progress} implementation
     * reads only volatile or atomic counters, so a stale read is the worst that can happen and that is fine for a
     * progress report.
     * <p/>
     * Note that {@link #barge(ReentrantReadWriteLock.ReadLock)} would <i>not</i> solve this: barging only skips ahead
     * of queued writers, and here the writer already holds the lock.
     *
     * @return the current delegate, never waiting for a concurrent flip.
     */
    private IndexProxy observeDelegate() {
        return delegate;
    }

    @Override
    public IndexDescriptor getDescriptor() {
        return observeDelegate().getDescriptor();
    }

    /**
     * Deliberately does <b>not</b> acquire the read lock, see {@link #observeDelegate()}.
     */
    @Override
    public InternalIndexState getState() {
        return observeDelegate().getState();
    }

    @Override
    public void close(CursorContext cursorContext) throws IOException {
        lock.readLock().lock();
        try {
            closed = true;
            delegate.close(cursorContext);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public ValueIndexReader newValueReader() throws IndexNotFoundKernelException {
        lock.readLock().lock();
        try {
            return delegate.newValueReader();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public TokenIndexReader newTokenReader() throws IndexNotFoundKernelException {
        lock.readLock().lock();
        try {
            return delegate.newTokenReader();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public boolean awaitStoreScanCompleted(long time, TimeUnit unit)
            throws IndexPopulationFailedKernelException, InterruptedException {
        IndexProxy proxy;
        lock.readLock().lock();
        proxy = delegate;
        lock.readLock().unlock();
        if (closed) {
            return false;
        }
        boolean stillGoing = proxy.awaitStoreScanCompleted(time, unit);
        if (!stillGoing) {
            // The waiting has ended. However we're not done because say that the delegate typically is a populating
            // proxy, when the wait is over
            // the populating proxy flips into something else, and if that is a failed proxy then that failure should
            // propagate out from this call.
            lock.readLock().lock();
            proxy = delegate;
            lock.readLock().unlock();
            proxy.awaitStoreScanCompleted(time, unit);
        }
        return stillGoing;
    }

    @Override
    public void activate() {
        // use write lock, since activate() might call flip*() which acquires a write lock itself.
        lock.writeLock().lock();
        try {
            delegate.activate();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void validate() throws IndexPopulationFailedKernelException, IncompleteConstraintValidationException {
        lock.readLock().lock();
        try {
            delegate.validate();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void validateBeforeCommit(Value[] tuple, long entityId) {
        lock.readLock().lock();
        try {
            delegate.validateBeforeCommit(tuple, entityId);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public ResourceIterator<Path> snapshotFiles() throws IOException {
        lock.readLock().lock();
        try {
            return delegate.snapshotFiles();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Map<String, Value> indexConfig() {
        lock.readLock().lock();
        try {
            return delegate.indexConfig();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Deliberately does <b>not</b> acquire the read lock, see {@link #observeDelegate()}.
     */
    @Override
    public IndexPopulationFailure getPopulationFailure() throws IllegalStateException {
        return observeDelegate().getPopulationFailure();
    }

    /**
     * Deliberately does <b>not</b> acquire the read lock, see {@link #observeDelegate()}.
     */
    @Override
    public PopulationProgress getIndexPopulationProgress() {
        return observeDelegate().getIndexPopulationProgress();
    }

    @Override
    public void maintenance() {
        lock.readLock().lock();
        try {
            delegate.maintenance();
        } finally {
            lock.readLock().unlock();
        }
    }

    void setFlipTarget(IndexProxyFactory flipTarget) {
        lock.writeLock().lock();
        try {
            this.flipTarget = flipTarget;
        } finally {
            lock.writeLock().unlock();
        }
    }

    void flipTo(IndexProxy targetDelegate) {
        lock.writeLock().lock();
        try {
            this.delegate = targetDelegate;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void flip(Callable<Boolean> actionDuringFlip)
            throws IndexProxyAlreadyClosedKernelException, ExceptionDuringFlipKernelException {
        lock.writeLock().lock();
        try {
            assertOpen();
            try {
                if (actionDuringFlip.call()) {
                    this.delegate = flipTarget.create();

                    if (started) {
                        this.delegate.start();
                    }
                }
            } catch (Exception e) {
                throw ExceptionDuringFlipKernelException.internalError(
                        this.getClass().getSimpleName(), e);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " -> " + delegate + "[target:" + flipTarget + "]";
    }

    private void assertOpen() throws IndexProxyAlreadyClosedKernelException {
        if (closed) {
            throw IndexProxyAlreadyClosedKernelException.internalError(this.getClass());
        }
    }

    private class LockingIndexUpdater extends DelegatingIndexUpdater {
        private LockingIndexUpdater(IndexUpdater delegate) {
            super(delegate);
            lock.readLock().lock();
        }

        @Override
        public void close() throws IndexEntryConflictException {
            try {
                delegate.close();
            } finally {
                lock.readLock().unlock();
            }
        }
    }
}
