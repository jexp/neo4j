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

import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.neo4j.lock.Lock;
import org.neo4j.lock.LockService;
import org.neo4j.util.VisibleForTesting;

/**
 * A database-local barrier that makes a kernel version upgrade a strong ordering point for write transactions.
 * <p>
 * A write transaction takes the shared side ({@link #enter()}) at the moment it captures the {@code KernelVersion} it
 * will stamp on its command batch(es), and holds it until it has finished appending them.
 * The upgrade path takes the exclusive side ({@link #lockForUpgrade()}) before replicating the version upgrade.
 * Because the two sides conflict:
 * <ul>
 *     <li>an upgrade drains in-flight version captures before it proceeds, and</li>
 *     <li>a version capture that starts while an upgrade holds the exclusive side blocks until the upgrade completes,
 *     and therefore reads the new version.</li>
 * </ul>
 * As a result no old-version batch can be appended to the (raft) log after the upgrade.
 * <p>
 * Unlike the entity {@code LockManager}, this barrier is honoured under MVCC, which deliberately ignores
 * node/relationship locks. It is only meaningful for the raft-triggered upgrade otherwise the {@link #NO_OP} is used
 * Upon successful consensus on the upgrade ({@link #upgradeLockNotRequired()} should be called
 * to remove the locking overhead
 */
public interface RaftUpgradeBarrier {
    /**
     * Taken by a write transaction that is about to capture the kernel version to stamp on its batches.
     * @return Returns a lock instance that must be closed upon completion of all transaction batches
     */
    Lock enter();

    /**
     * Taken by the upgrade path before replicating the version upgrade. Blocks new version captures and waits for
     * in-flight ones to finish.
     * Once upgrade is completed successfully it should never be called again
     */
    void lockForUpgrade() throws InterruptedException;

    /**
     * Released once the version upgrade has been replicated and applied.
     */
    void unlockAfterUpgrade();

    /**
     * If not already a NO_OP lock then replace with that. Any active transaction committers will still have a local
     * lock to release, but future lockers will be given a NO-OP instance.
     */
    void upgradeLockNotRequired();

    /**
     * Test only method that shows an estimate of if a thread is trying to enter the exclusive lock,
     * or has successfully claimed the lock and not yet released it.
     * @return returns true if there is a caller to {@link #lockForUpgrade()} and not yet a call to {@link #unlockAfterUpgrade()}
     */
    @VisibleForTesting
    boolean upgraderPresent();

    /**
     * @param enabled whether the barrier should actually guard version captures (raft-triggered upgrade of a
     * multi-versioned database). When {@code false}, {@link #NO_OP} is returned.
     */
    static RaftUpgradeBarrier create(boolean enabled) {
        return enabled ? new ReadWriteRaftUpgradeBarrier() : NO_OP;
    }

    /**
     * A barrier that does nothing, used when there is no possibility of a concurrent kernel version upgrade racing a
     * version capture. Its exclusive side is always "acquired" so an upgrade never blocks on it.
     */
    RaftUpgradeBarrier NO_OP = new RaftUpgradeBarrier() {
        @Override
        public Lock enter() {
            return LockService.NO_LOCK;
        }

        @Override
        public void lockForUpgrade() {
            throw new IllegalStateException("Scheduled raft upgrade should not be installed");
        }

        @Override
        public void unlockAfterUpgrade() {
            throw new IllegalStateException("Scheduled raft upgrade should not be installed");
        }

        @Override
        public void upgradeLockNotRequired() {
            // already a NO-OP
        }

        @Override
        @VisibleForTesting
        public boolean upgraderPresent() {
            return false;
        }
    };
}

final class ReadWriteRaftUpgradeBarrier implements RaftUpgradeBarrier {
    private volatile ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private volatile boolean upgraderPresent = false;

    @Override
    public Lock enter() {
        final ReentrantReadWriteLock sharedLock = lock;
        if (sharedLock == null) {
            return LockService.NO_LOCK;
        }
        final ReentrantReadWriteLock.ReadLock locker = sharedLock.readLock();
        locker.lock();
        return new Lock() {
            @Override
            public void release() {
                locker.unlock();
            }
        };
    }

    @Override
    public void lockForUpgrade() throws InterruptedException {
        final ReentrantReadWriteLock sharedLock = lock;
        if (sharedLock == null) {
            throw new IllegalStateException("Upgrade has already happened, should not occur again");
        }
        upgraderPresent = true;
        sharedLock.writeLock().lockInterruptibly();
    }

    @Override
    public void unlockAfterUpgrade() {
        final ReentrantReadWriteLock sharedLock = lock;
        if (sharedLock == null) {
            throw new IllegalStateException("Upgrade has already happened, should not occur again");
        }
        sharedLock.writeLock().unlock();
        upgraderPresent = false;
    }

    @Override
    public void upgradeLockNotRequired() {
        // Replace the RW lock instance. Any in flight shared locks will still have a local reference
        // and unlock that, but future callers will be handed a NO-OP instance
        lock = null;
    }

    @Override
    @VisibleForTesting
    public boolean upgraderPresent() {
        return upgraderPresent;
    }
}
