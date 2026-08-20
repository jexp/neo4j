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
package org.neo4j.fleetmanagement.configuration;

import org.neo4j.fleetmanagement.communication.upstream.Upstream;
import org.neo4j.fleetmanagement.transactions.ITransactor;

public class ClusterSync {
    private final ITransactor transactor;
    private final Upstream upstream;
    private final State state;

    public ClusterSync(ITransactor transactor, Upstream upstream, State state) {
        this.transactor = transactor;
        this.upstream = upstream;
        this.state = state;
    }

    public void run() {
        this.state.setActive(this.transactor.getTokenStatus());
        this.state.setRotatingToken(this.transactor.getTokenRotationStatus());
        syncToken();
    }

    private void syncToken() {
        var tokenAndConnectionUrl = this.transactor.getTokenAndConnectionUrl();
        if (tokenAndConnectionUrl == null || tokenAndConnectionUrl.token == null) {
            return;
        }

        try {
            this.upstream.setToken(tokenAndConnectionUrl.token);
            this.upstream.setConnectionUrl(tokenAndConnectionUrl.connectionUrl);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
