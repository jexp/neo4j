/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.expressions.functions

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.expressions.FunctionTypeSignature
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTDate
import org.neo4j.cypher.internal.util.symbols.CTDateTime
import org.neo4j.cypher.internal.util.symbols.CTDuration
import org.neo4j.cypher.internal.util.symbols.CTFloat
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTLocalDateTime
import org.neo4j.cypher.internal.util.symbols.CTLocalTime
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTPath
import org.neo4j.cypher.internal.util.symbols.CTPoint
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.CTTime
import org.neo4j.cypher.internal.util.symbols.CTUUID
import org.neo4j.cypher.internal.util.symbols.CTVector

case object ToString extends Function {
  override def name = "toString"

  val validInputTypesCypher5 = Seq(
    CTFloat,
    CTInteger,
    CTBoolean,
    CTString,
    CTUUID, // Cypher 5 pass through support
    CTDuration,
    CTDate,
    CTTime,
    CTDateTime,
    CTLocalTime,
    CTLocalDateTime,
    CTPoint,
    CTVector // Cypher 5 pass through support
  )

  val validInputTypes = validInputTypesCypher5 ++ Seq(
    CTList(CTAny),
    CTMap,
    CTNode,
    CTRelationship,
    CTPath
  )

  override val signatures: Vector[FunctionTypeSignature] = Vector(
    FunctionTypeSignature(
      this,
      names = Vector("input"),
      argumentTypes = Vector(CTAny),
      outputType = CTString,
      description =
        "Converts an `INTEGER`, `FLOAT`, `BOOLEAN`, `POINT` or temporal type (i.e. `DATE`, `ZONED TIME`, `LOCAL TIME`, `ZONED DATETIME`, `LOCAL DATETIME` or `DURATION`) value to a `STRING`.",
      category = Category.STRING,
      argumentDescriptions = Map("input" -> "A value to be converted into a string."),
      scopes = Set(CypherVersion.Cypher5)
    ),
    FunctionTypeSignature(
      this,
      names = Vector("input"),
      argumentTypes = Vector(CTAny),
      outputType = CTString,
      description =
        "Converts a value to a `STRING`.",
      category = Category.STRING,
      argumentDescriptions = Map("input" -> "A value to be converted into a string."),
      scopes = Set(CypherVersion.Cypher25)
    )
  )
}
