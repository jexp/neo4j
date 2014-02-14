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
package org.neo4j.cypher.internal.compiler.v2_0.ast.rewriters

import org.neo4j.cypher.internal.compiler.v2_0._
import ast._

case class ExtractParameters(replaceableLiterals: Map[(Any, InputPosition), Parameter]) extends Rewriter {
  def apply(term: Any): Option[Any] = rewriter.apply(term)

  private val rewriter: Rewriter = Rewriter.lift {
    case l: Literal =>
      replaceableLiterals.get((l.value, l.position)).getOrElse(l)
  }
}

object ExtractParameters {

  private val literalMatcher: PartialFunction[Any, Map[(Any,InputPosition), ast.Parameter] => Map[(Any,InputPosition), ast.Parameter]] = {
    case l: ast.Literal => _ + ((l.value, l.position) -> ast.Parameter(s"  AUTO${l.position.offset}")(l.position))
  }

  def prepare(statement: ast.Statement): (Rewriter, Map[String, Any]) = {
    val replaceableLiterals = statement.fold(Map.empty[(Any,InputPosition), ast.Parameter]) {
      case n: ast.NodePattern =>
        acc => n.properties.fold(Map.empty[(Any,InputPosition), ast.Parameter])(_.fold(acc)(literalMatcher))
      case r: ast.RelationshipPattern =>
        acc => r.properties.fold(Map.empty[(Any,InputPosition), ast.Parameter])(_.fold(acc)(literalMatcher))
      case u: ast.UpdateClause =>
        acc => u.fold(acc)(literalMatcher)
      case w: ast.Where =>
        acc => w.fold(acc)(literalMatcher)
      case r: ast.Return =>
        acc => r.fold(acc)(literalMatcher)
      case w: ast.With =>
        acc => w.fold(acc)(literalMatcher)
    }

    (ExtractParameters(replaceableLiterals), replaceableLiterals.map {
      case (l, p) => (p.name, l._1)
      }
    )
  }
}
