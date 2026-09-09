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
package org.neo4j.cypher.internal.frontend.phases.parserTransformers

import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.CountExpression
import org.neo4j.cypher.internal.ast.ExistsExpression
import org.neo4j.cypher.internal.ast.FreeProjection
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.helpers.LazyVal
import org.neo4j.cypher.internal.util.topDown

/**
 * Removes the projections that expanding clauses leaves behind: a WITH that neither projects nor filters
 * has no effect on the query. A subquery expression emptied out this way gets a placeholder projection
 * instead, since it still has to produce a row to be evaluated.
 *
 * Only used for Cypher 25: Cypher 5 needs a WITH between clauses in some cases.
 */
case object DropEmptyProjections {

  def rewriter(anonVarNameGen: AnonymousVariableNameGenerator): Rewriter = {
    val placeholderName = LazyVal(anonVarNameGen.nextName)

    def keptClauses(clauses: Seq[Clause]): Seq[Clause] = clauses.filter {
      case With(false, ReturnItems(_, Seq(), _), None, None, None, None, None, _) => false
      case _                                                                      => true
    }

    def placeholderProjection(sq: SingleQuery): SingleQuery =
      sq.copy(Seq(Return(
        ReturnItems(
          FreeProjection,
          Seq(AliasedReturnItem(
            SignedDecimalIntegerLiteral("1")(sq.position.zeroLength),
            Variable(placeholderName.value, sq.position)
          )(sq.position, AliasedReturnItem.wasAutoAliasedDefault))
        )(sq.position)
      )(sq.position)))(sq.position)

    def inSubqueryExpression(query: Query): Query =
      query.mapEachSingleQuery { sq =>
        val clauses = keptClauses(sq.clauses)
        if (clauses.nonEmpty) sq.copy(clauses)(sq.position)
        else placeholderProjection(sq)
      }

    topDown(Rewriter.lift({
      case ex: ExistsExpression =>
        ex.copy(query = inSubqueryExpression(ex.query))(
          ex.position,
          ex.computedIntroducedVariables,
          ex.computedScopeDependencies
        )
      case cnt: CountExpression =>
        cnt.copy(query = inSubqueryExpression(cnt.query))(
          cnt.position,
          cnt.computedIntroducedVariables,
          cnt.computedScopeDependencies
        )
      case sq: SingleQuery => sq.copy(keptClauses(sq.clauses))(sq.position)
    }))
  }
}
