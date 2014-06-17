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
package org.neo4j.cypher

import java.io.{PrintWriter, File}
import org.neo4j.cypher.internal.commons.CreateTempFileTestSupport
import org.neo4j.cypher.internal.compiler.v2_1.commands.expressions.StringHelper.RichString
import org.neo4j.test.TestGraphDatabaseFactory
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.scalatest.BeforeAndAfterAll
import org.codehaus.jackson.map.ObjectMapper
import collection.JavaConverters._
import org.codehaus.jackson.JsonParseException

class LoadJsonAcceptanceTest
  extends ExecutionEngineFunSuite with BeforeAndAfterAll
  with QueryStatisticsTestSupport with CreateTempFileTestSupport {

  val mapper = new ObjectMapper()

  def toJson(value: Iterable[Map[String,Any]]) = mapper.writeValueAsString(value.map(_.asJava).asJava)
  def toJson(value:Map[String,Any]) = mapper.writeValueAsString(value.asJava)

  test("import three maps") {
    val url = createJSONTempFileURL({
      writer =>
        writer.println(toJson(List(Map("name" ->"Foo1"),Map("name" ->"Foo2"),Map("name" ->"Foo2"))))
    }).cypherEscape

    val result = execute(s"LOAD JSON FROM '$url' AS line CREATE (a {name: line.name}) RETURN a.name")
    assertStats(result, nodesCreated = 3, propertiesSet = 3)
  }

  test("import single map") {
    val url = createJSONTempFileURL({
      writer =>
        writer.println(toJson(Map("name" ->"Foo1")))
    }).cypherEscape

    val result = execute(s"LOAD JSON FROM '$url' AS line CREATE (a {name: line.name}) RETURN a.name")
    assertStats(result, nodesCreated = 1, propertiesSet = 1)
  }

  test("import a nested array") {
    val url = createJSONTempFileURL({
      writer =>
        writer.println("{ \"topics\": [{\"name\": \"Neo4j\"}, {\"name\": \"Java\"}]}")
    }).cypherEscape

    val result = execute(s"LOAD JSON FROM '$url' AS line WITH line UNWIND line.topics AS topic CREATE (t {name: topic.name })")

    assertStats(result, nodesCreated = 2, propertiesSet = 2)
  }

  test("import a nested nested array") {
    val url = createJSONTempFileURL({
      writer =>
        writer.println("[ {\"name\": \"Michael\", \"topics\": [\"Java\", \"Neo4j\"]} ]")
    }).cypherEscape

    val result = execute(s""" LOAD JSON FROM '$url' AS line
                              CREATE (p { name: line.name, topics: line.topics } )
                              WITH p, line
                              UNWIND line.topics AS topic
                              CREATE (p)-[:INTERESTED_IN]->(t { name: topic } )""")

    assertStats(result, nodesCreated = 3, propertiesSet = 4, relationshipsCreated = 2)
  }

  test("handles new line character") {
    val url = createJSONTempFileURL({
      writer =>
        writer.println("[ {\"name\": \"Michael\",\n\t \"topics\": [\"Java\", \"Neo4j\"]} ]")
    }).cypherEscape

    val result = execute(s""" LOAD JSON FROM '$url' AS line
                              CREATE (p { name: line.name, topics: line.topics } )
                              WITH p, line
                              UNWIND line.topics AS topic
                              CREATE (p)-[:INTERESTED_IN]->(t { name: topic } )""")

    assertStats(result, nodesCreated = 3, propertiesSet = 4, relationshipsCreated = 2)
  }



  test("throws CypherTypeException on array of array") {
    val url = createJSONTempFileURL({
      writer =>
        writer.println("[[1,2,3]]")
    }).cypherEscape

    val exception = intercept[CypherTypeException] {
      execute(s""" LOAD JSON FROM '$url' AS line RETURN line""")
    }

    exception.getMessage should equal("Unexpected token while parsing json stream START_ARRAY. LOAD JSON only supports JSON objects and arrays thereof.")
  }

  test("throws JsonParseException on invalid JSON") {
    val url = createJSONTempFileURL({
      writer =>
        writer.println("{notevenvalid}")
    }).cypherEscape

    val exception = intercept[JsonParseException] {
      execute(s""" LOAD JSON FROM '$url' AS line RETURN line""")
    }

    exception.getMessage should startWith("Unexpected character ('n' (code 110)): was expecting double-quote to start field name")
  }


}
