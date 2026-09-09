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
package org.neo4j.cypher.internal.frontend.helpers

import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.ExpandClauses
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.ExpandStarProjections
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.ComputeExpressionDependencies
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.ScopeSurveyor

object ExpandClausesTestUtil {

  /**
   * The sequence the parse pipeline derives from the declared conditions, spelled out for tests that wire
   * these phases by hand instead of going through the StepSequencer.
   */
  val expandStarsAndClauses: Transformer[BaseContext, BaseState, BaseState] =
    ExpandStarProjections andThen ScopeSurveyor andThen ComputeExpressionDependencies andThen ExpandClauses
}
