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
package org.neo4j.cypher.docgen

import org.neo4j.cypher.QueryStatisticsTestSupport
import org.neo4j.visualization.graphviz.{AsciiDocSimpleStyle, GraphStyle}
import org.junit.Test
import java.io.File
import org.junit.Assert._

class LoadJSONTest extends DocumentingTestBase with QueryStatisticsTestSupport {
  override protected def getGraphvizStyle: GraphStyle =
    AsciiDocSimpleStyle.withAutomaticRelationshipTypeColors()

  implicit var jsonFilesDir: File = createDir(dir, "json-files")

  def section = "Load JSON"

  private val tomHanks = new JsonFile("tomHanks.json").withContentsF(
    """
      {"name": "Tom Hanks",
       "movies": [{"title": "Saving Mr. Banks", "year": 2013}, {"title": "Toy Story 3", "year": 2010}]}
    """
  )

  private val keanuReeves = new JsonFile("keanuReeves.json").withContentsF(
    """
      {"name": "Keanu Reeves",
       "movies": [{"title": "Dracula", "year": 1992}, {"title": "Sweet November", "year": 2001}]}
    """
  )

  private val actors = new JsonFile("actors.json").withContentsF(
    """
      [{"name": "Tom Hanks",
       "movies": [{"title": "Saving Mr. Banks", "year": 2013}, {"title": "Toy Story 3", "year": 2010}]},
       {"name": "Emma Thompson",
        "movies": [{"title": "Saving Mr. Banks", "year": 2013}, {"title": "Men In Black 3", "year": 2012}]}]
    """
  )

  filePaths = Map(
    "%ACTOR%" -> URIHelper.urify(tomHanks),
    "%ACTORS%" -> URIHelper.urify(actors),
    "%BASE%" -> URIHelper.urify(jsonFilesDir)
  )

  urls = Map(
    "%ACTOR%" -> (baseUrl + tomHanks.getName),
    "%ACTORS%" -> (baseUrl + actors.getName),
    "%BASE%" -> baseUrl
  )

  @Test def should_import_data_from_a_json_file() {
    testQuery(
      title = "Import data from a JSON file",
      text = """
To import data from a JSON file into Neo4j, you can use +LOAD JSON+ to get the data into your query.
Then you write it to your database using the normal updating clauses of Cypher.

.tomHanks.json
[source]
----
include::json-files/tomHanks.json[]
----
""",
      queryText =
        """LOAD JSON FROM '%ACTOR%' AS document
           CREATE (actor:Actor {name: document.name})
           WITH actor, document
           UNWIND document.movies AS movie
           MERGE (m:Movie {title: movie.title, year: movie.year})
           CREATE (actor)-[:ACTED_IN]->(m)""",
      optionalResultExplanation =
        """
A new node with the +Actor+ label is created and the 'name' property from the document is set as a property on the node.
        In addition 2 nodes with the +Movie+ label are created with +title+ and +year+ properties set. Relationships are
        created between the +Actor+ nodes and +Movie+ nodes.""",
      assertions = (p) => assertStats(p, nodesCreated = 3, relationshipsCreated=2, propertiesSet = 5, labelsAdded = 3))
  }

  @Test def should_import_data_from_a_json_array() {
    testQuery(
      title = "Import data from a JSON file containing multiple JSON documents",
      text = """
You can also provide an array of JSON documents to import multiple documents at one time.

.actors.json
[source]
----
include::json-files/actors.json[]
----
             """,
      queryText =
        """LOAD JSON FROM '%ACTORS%' AS document CREATE (actor:Actor {name: document.name})
           WITH actor, document
           UNWIND document.movies AS movie
           MERGE (m:Movie {title: movie.title, year: movie.year})
           CREATE (actor)-[:ACTED_IN]->(m)""",
      optionalResultExplanation =
        """
Two new nodes with the +Actor+ label are created and the 'name' property from the document is set as a property on the nodes.
        In addition 3 nodes with the +Movie+ label are created with +title+ and +year+ properties set.""",
      assertions = (p) => assertStats(p, nodesCreated = 5, relationshipsCreated=4, propertiesSet = 8, labelsAdded = 5))
  }

  @Test def should_import_data_from_multiple_json_files() {
    testQuery(
      title = "Import data from multiple JSON files",
      text = """
You can also make use of +UNWIND+ to process multiple JSON files at once.

.tomHanks.json
[source]
----
include::json-files/tomHanks.json[]
----

.keanuReeves.json
[source]
----
include::json-files/keanuReeves.json[]
----

             """,
      queryText =
        """UNWIND ["tomHanks.json","keanuReeves.json"] AS fileName
           LOAD JSON FROM "%BASE%" + fileName AS document CREATE (actor:Actor {name: document.name})
           WITH actor, document
           UNWIND document.movies AS movie
           MERGE (m:Movie {title: movie.title, year: movie.year})
           CREATE (actor)-[:ACTED_IN]->(m)""",
      optionalResultExplanation =
        """
Two new nodes with the +Actor+ label are created and the 'name' property from the document is set as a property on the nodes.
        In addition 3 nodes with the +Movie+ label are created with +title+ and +year+ properties set.""",
      assertions = (p) => assertStats(p, nodesCreated = 6, relationshipsCreated=4, propertiesSet = 10, labelsAdded = 6))
  }



}
