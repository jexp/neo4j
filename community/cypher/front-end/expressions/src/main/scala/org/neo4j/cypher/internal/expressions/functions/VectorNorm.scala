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
import org.neo4j.cypher.internal.util.symbols.CTFloat
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.CTVector

case object VectorNorm extends Function {
  override def name = "vector_norm"

  override val signatures: Vector[FunctionTypeSignature] = Vector(
    FunctionTypeSignature(
      this,
      names = Vector("vector", "vectorDistanceMetric"),
      argumentTypes =
        Vector(CTVector, CTString),
      outputType = CTFloat,
      description =
        "Returns a `FLOAT` representing the distance between the given vector and an origin vector, which is a vector with the same dimension with all coordinates set to zero, calculated using the specified `vectorDistanceMetric`.",
      overrideDefaultAsString = Some(
        name + "(vector :: VECTOR, vectorDistanceMetric :: [EUCLIDEAN, MANHATTAN]) :: FLOAT"
      ),
      overriddenArgumentTypeName =
        Some(Map("vectorDistanceMetric" -> "[EUCLIDEAN, MANHATTAN]")),
      category = Category.VECTOR,
      argumentDescriptions = Map(
        "vector" -> "A vector for which the norm to the origin vector will be computed.",
        "vectorDistanceMetric" -> "The vector distance algorithm to calculate the distance by."
      ),
      scopes = Set(CypherVersion.Cypher25)
    )
  )
}
