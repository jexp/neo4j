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
package org.neo4j.cypher.internal.compiler.v2_1.spi

import java.net._
import java.io._
import java.util.zip.{DeflaterInputStream, GZIPInputStream}
import au.com.bytecode.opencsv.CSVReader
import org.neo4j.cypher.internal.compiler.v2_1.TaskCloser
import org.neo4j.cypher.{CypherTypeException, LoadJsonStatusWrapCypherException, LoadExternalResourceException}
import org.neo4j.cypher.internal.compiler.v2_1.pipes.ExternalResource
import org.codehaus.jackson.map.ObjectMapper
import scala.util.parsing.json._
import collection.JavaConverters._
import org.codehaus.jackson.{JsonParser, JsonToken, JsonFactory}
import java.util
import org.codehaus.jackson.`type`.TypeReference
import scala.collection.AbstractIterator

//todo rename to DataResources
object CSVResources {
  val DEFAULT_FIELD_TERMINATOR: Char = ','

  val mapper = new ObjectMapper()

  val jsonFactory = new JsonFactory(mapper)

  val mapTypeReference = new TypeReference[util.Map[String, Object]]() {}

  def asScalaRecursive(value : Any) : Any = value match {
    case x:java.util.Map[String, Any] => x.asScala.transform((key, value) => asScalaRecursive(value) ).toMap
    case x:java.lang.Iterable[Any] => x.asScala.map(asScalaRecursive)
    case x => x
  }

  def readValue(parser:JsonParser) : Map[String,Any] = {
    val value = asScalaRecursive(parser.readValueAs(mapTypeReference))
    parser.nextToken()
    value.asInstanceOf[Map[String,Any]]
  }

}

class CSVResources(cleaner: TaskCloser) extends ExternalResource {

  def getCsvIterator(url: URL, fieldTerminator: Option[String] = None): Iterator[Array[String]] = {
    val inputStream = openStream(url)
    val reader = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"))
    val csvReader = new CSVReader(reader, fieldTerminator.map(_.charAt(0)).getOrElse(CSVResources.DEFAULT_FIELD_TERMINATOR))

    cleaner.addTask(_ => {
      csvReader.close()
    })

    new Iterator[Array[String]] {
      var nextRow: Array[String] = csvReader.readNext()

      def hasNext: Boolean = nextRow != null

      def next(): Array[String] = {
        if (!hasNext) Iterator.empty.next()
        val row = nextRow
        nextRow = csvReader.readNext()
        row
      }
    }
  }

  private def openStream(url: URL, connectionTimeout: Int = 2000, readTimeout: Int = 10 * 60 * 1000): InputStream = {
    try {
      if (url.getProtocol.startsWith("http"))
        TheCookieManager.ensureEnabled()
      val con = url.openConnection()
      con.setConnectTimeout(connectionTimeout)
      con.setReadTimeout(readTimeout)
      val stream = con.getInputStream()
      con.getContentEncoding match {
        case "gzip" => new GZIPInputStream(stream)
        case "deflate" => new DeflaterInputStream(stream)
        case _ => stream
      }
    } catch {
      case e: IOException =>
        throw new LoadExternalResourceException(s"Couldn't load the external resource at: $url", e)
    }
  }

  def getJsonIterator(url: URL): Iterator[Map[String, Any]] = {

    val inputStream = openStream(url)
    val reader = new BufferedReader(new InputStreamReader(inputStream))

    cleaner.addTask((_) => {
      reader.close()
    })

    val parser = CSVResources.jsonFactory.createJsonParser(reader)
    val token = parser.nextToken()

    token match {
      case JsonToken.START_ARRAY => {
        parser.nextToken()
        new Iterator[Map[String, Any]] {
          def hasNext = parser.getCurrentToken match {
            case JsonToken.START_OBJECT => true
            case JsonToken.END_ARRAY => false
            case x => throw new CypherTypeException("Unexpected token while parsing json stream " + x  + ". LOAD JSON only supports JSON objects and arrays thereof.")
          }
          def next() = CSVResources.readValue(parser)
        }
      }
      case JsonToken.START_OBJECT => Iterator.single(CSVResources.readValue(parser))
      case x => throw new CypherTypeException("Unexpected token while parsing json stream " + x)
    }
  }
}

object TheCookieManager {
  private lazy val theCookieManager = create

  def ensureEnabled() {
    // Force lazy val to be evaluated
    theCookieManager != null
  }

  private def create = {
    val cookieManager = new CookieManager
    CookieHandler.setDefault(cookieManager)
    cookieManager.setCookiePolicy(CookiePolicy.ACCEPT_ALL)
    cookieManager
  }
}

