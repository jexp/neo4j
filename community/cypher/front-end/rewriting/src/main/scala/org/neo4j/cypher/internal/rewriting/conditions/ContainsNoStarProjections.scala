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
package org.neo4j.cypher.internal.rewriting.conditions

import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.ScopeClauseSubqueryCall
import org.neo4j.cypher.internal.util.ASTNode

case object ContainsNoStarProjections extends ContainsNoMatchingStatementNodes {

  override val matcher: PartialFunction[ASTNode, String] = {
    case ri: ReturnItems if ri.includeExisting            => "ReturnItems(includeExisting = true, ...)"
    case sq: ScopeClauseSubqueryCall if sq.isImportingAll => "ScopeClauseSubqueryCall(isImportingAll = true, ...)"
  }

  override val name: String = "NoStarProjections"
}
