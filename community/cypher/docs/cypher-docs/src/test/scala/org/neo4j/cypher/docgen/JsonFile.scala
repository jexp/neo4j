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

import java.io.{PrintWriter, File}
import scala.util.parsing.json.JSONObject
import org.codehaus.jackson.map.ObjectMapper
import org.codehaus.jackson.JsonNode


class JsonFile(fileName: String)(implicit jsonFilesDir: File) {

  import URIHelper._

  def withContents(document: String): String = {
    val jsonFile = withContentsF(document)
    urify(jsonFile)
  }

  def withContentsF(document: String): File = {
    val jsonFile = new File(jsonFilesDir, fileName)
    val writer = new PrintWriter(jsonFile, "UTF-8")

    val mapper = new ObjectMapper()
    val tree = mapper.readTree(document)
    val pretty = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(tree)

    writer.print(pretty)
    writer.flush()
    writer.close()
    jsonFile
  }
}
