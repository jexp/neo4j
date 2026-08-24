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
package org.neo4j.wal;

import java.nio.file.Path;
import org.neo4j.internal.nativeimpl.NativeAccess;
import org.neo4j.io.fs.ChannelNativeAccessor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;

public class StoreChannelNativeAccessor implements ChannelNativeAccessor {
    private final FileSystemAbstraction fileSystem;
    private final NativeAccess nativeAccess;
    private final InternalLog log;
    private final OutOfDiskHandler onOutOfDisk;

    public StoreChannelNativeAccessor(
            FileSystemAbstraction fileSystem,
            NativeAccess nativeAccess,
            InternalLogProvider logProvider,
            OutOfDiskHandler onOutOfDisk) {
        this.fileSystem = fileSystem;
        this.nativeAccess = nativeAccess;
        this.log = logProvider.getLog(getClass());
        this.onOutOfDisk = onOutOfDisk;
    }

    @Override
    public void adviseSequentialAccessAndKeepInCache(StoreChannel channel, Path file) {
        if (channel.isOpen()) {
            final int fileDescriptor = fileSystem.getFileDescriptor(channel);
            var sequentialResult = nativeAccess.tryAdviseSequentialAccess(fileDescriptor);
            if (sequentialResult.isError()) {
                log.warn("Unable to advise sequential access for file " + file + ". Error: " + sequentialResult);
            }
            var cacheResult = nativeAccess.tryAdviseToKeepInCache(fileDescriptor);
            if (cacheResult.isError()) {
                log.warn("Unable to advise preserve data in cache for file " + file + ". Error: " + cacheResult);
            }
        }
    }

    @Override
    public void evictFromSystemCache(StoreChannel channel, Path file) {
        if (channel.isOpen()) {
            var result = nativeAccess.tryEvictFromCache(fileSystem.getFileDescriptor(channel));
            if (result.isError()) {
                log.warn("Unable to evict file " + file + " from cache . Error: " + result);
            }
        }
    }

    @Override
    public void preallocateSpace(StoreChannel storeChannel, long bytes, Path file) {
        int fileDescriptor = fileSystem.getFileDescriptor(storeChannel);
        var result = nativeAccess.tryPreallocateSpace(fileDescriptor, bytes);
        if (result.isError()) {
            if (nativeAccess.errorTranslator().isOutOfDiskSpace(result)) {
                onOutOfDisk.handle(result.toString());
            } else {
                log.warn("Error on attempt to preallocate file " + file + ". Error: " + result);
            }
        }
    }
}
