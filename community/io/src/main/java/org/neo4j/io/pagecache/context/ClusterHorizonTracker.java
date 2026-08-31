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
package org.neo4j.io.pagecache.context;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.neo4j.io.pagecache.context.ClusterHorizonTracker.ClusterHorizonTrackerImpl.HorizonPin;

public interface ClusterHorizonTracker {
    ClusterHorizonTracker NO_OP = new ClusterHorizonTracker() {
        @Override
        public HorizonPin pin(long oldestVisibilityHorizon) {
            return null;
        }

        @Override
        public void unpin(HorizonPin horizonPin) {}

        @Override
        public long oldestVisibilityHorizon() {
            return Long.MAX_VALUE;
        }
    };

    HorizonPin pin(long oldestVisibilityHorizon);

    void unpin(HorizonPin horizonPin);

    long oldestVisibilityHorizon();

    class ClusterHorizonTrackerImpl implements ClusterHorizonTracker {
        private final Set<HorizonPin> horizons = ConcurrentHashMap.newKeySet();

        @Override
        public HorizonPin pin(long oldestVisibilityHorizon) {
            HorizonPin horizonPin = new HorizonPin(oldestVisibilityHorizon);
            horizons.add(horizonPin);
            return horizonPin;
        }

        @Override
        public void unpin(HorizonPin horizonPin) {
            horizons.remove(horizonPin);
        }

        @Override
        public long oldestVisibilityHorizon() {
            return horizons.stream()
                    .mapToLong(HorizonPin::oldestVisibilityHorizon)
                    .min()
                    .orElse(Long.MIN_VALUE);
        }

        public static final class HorizonPin {
            private final long oldestVisibilityHorizon;

            private HorizonPin(long oldestVisibilityHorizon) {
                this.oldestVisibilityHorizon = oldestVisibilityHorizon;
            }

            public long oldestVisibilityHorizon() {
                return oldestVisibilityHorizon;
            }
        }
    }
}
