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
package org.neo4j.io.pagecache.segment;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PageCacheSegmentTracker implements DatabaseSegmentTracker {
    private final ConcurrentHashMap<Path, PageFileSegmentTracker> fileTrackers = new ConcurrentHashMap<>();
    private final Path databaseDirectory;

    public PageCacheSegmentTracker(Path databaseDirectory) {
        this.databaseDirectory = databaseDirectory;
    }

    @Override
    public FileSegmentTracker createFileSegmentTracer(Path baseSegmentPath) {
        PageFileSegmentTracker pageFileSegmentTracker = fileTrackers.get(baseSegmentPath);
        if (pageFileSegmentTracker != null) {
            return pageFileSegmentTracker;
        }
        return fileTrackers.computeIfAbsent(baseSegmentPath, path -> new PageFileSegmentTracker());
    }

    @Override
    public void removeFileSegmentTracer(Path baseSegmentPath) {
        fileTrackers.remove(baseSegmentPath);
    }

    @Override
    public Collection<FileSegmentMetadata> segmentMetadata() {
        List<FileSegmentMetadata> segmentsMeta = new ArrayList<>(fileTrackers.size());
        for (Map.Entry<Path, PageFileSegmentTracker> entry : fileTrackers.entrySet()) {
            PageFileSegmentTracker tracker = entry.getValue();
            segmentsMeta.add(new FileSegmentMetadata(
                    databaseDirectory.relativize(entry.getKey()).toString(),
                    tracker.segmentCount(),
                    tracker.drainChangedSegments()));
        }
        return segmentsMeta;
    }
}
