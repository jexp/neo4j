/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.doc.cypherdoc;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.cypher.javacompat.ExtendedExecutionResult;

class Result
{
    final String query;
    final String text;
    final String profile;
    final Set<Long> nodeIds = new HashSet<>();
    final Set<Long> relationshipIds = new HashSet<>();

    public Result(String query, ExtendedExecutionResult result) {
        this.query = query;
        for ( Map<String, Object> row : result )
        {
            // do something here
        }
        text = result.dumpToString();
        String profileText;
        try
        {
            profileText = result.executionPlanDescription().toString();
        }
        catch ( Exception ex )
        {
            profileText = ex.getMessage();
        }
        profile = profileText;
    }

    public Result( String query, String text )
    {
        this.query = query;
        this.text = text;
        this.profile = "";
    }
}
