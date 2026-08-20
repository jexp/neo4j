package org.neo4j.fleetmanagement.transactions;

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

import java.util.List;
import java.util.Map;
import org.neo4j.annotations.service.Service;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.fleetmanagement.topology.model.Database;
import org.neo4j.fleetmanagement.topology.model.GraphCount;
import org.neo4j.fleetmanagement.topology.model.Server;
import org.neo4j.fleetmanagement.transactions.model.TokenAndConnectionUrl;
import org.neo4j.fleetmanagement.transactions.model.VersionAndEdition;

@Service
public interface ITransactor {
    void init(DatabaseManagementService databaseManagementService);

    boolean getTokenStatus();

    boolean getTokenRotationStatus();

    TokenAndConnectionUrl getTokenAndConnectionUrl();

    VersionAndEdition getVersionAndEdition();

    Map<String, Server> getServers();

    Map<String, List<Database>> getDatabases();

    Server.License getLicense();

    Server.License getGdsLicense();

    Server.License getBloomLicense();

    GraphCount getGraphCount(String databaseName);

    void setToken(String token);

    void rotateToken();

    void deleteToken();
}
