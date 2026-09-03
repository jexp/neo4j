/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.cucumber.synthesise.generator

import org.neo4j.cypher.cucumber.glue.regular.CompositeExecutorPool
import org.neo4j.cypher.cucumber.synthesise.generator.Filter.doNotContainAst
import org.neo4j.cypher.internal.ast.ConditionalQueryWhen
import org.neo4j.cypher.internal.ast.NextStatement
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.QueryWithLocalDefinitions
import org.neo4j.cypher.internal.ast.ReturnItem
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.TopLevelBraces
import org.neo4j.cypher.internal.ast.Union
import org.neo4j.cypher.internal.ast.UseGraph

import scala.util.Try

/** Shared `USE <constituent>` routing + preparser-directive peeling for the composite generators. */
object CompositeUse {

  def usePrefixFor(constituent: String): String = s"USE $constituent\n"

  val usePrefix: String = usePrefixFor(CompositeExecutorPool.Constituent)

  // A leading CYPHER <version> directive / EXPLAIN / PROFILE must stay ahead of the injected USE, else the query runs
  // under the wrong language. Generators peel these, wrap the rest, and re-prepend.
  val leadingCypherOptions = "(?is)^(\\s*CYPHER\\s+(?:\\d+\\b\\s*)?(?:[a-zA-Z_.]+\\s*=\\s*\\S+\\s*)*)(.*)$".r
  val leadingExplainOrProfile = "(?is)^(\\s*(?:explain|profile)\\b\\s*)(.*)$".r

  /**
   * Inject `USE <constituent>`, dispatching on the top-level construct: plain/UNION -> as the first clause of every
   * leaf; WHEN/NEXT/local-defs -> `USE <c> { ... }` (a leading USE can't precede WHEN nor a non-first NEXT operand);
   * top-level braces -> attach to the existing braces. Splices into the original text; falls back to a plain prefix
   * if the query does not parse.
   */
  def route(parser: CachingParser, usePrefix: String, cypher: String): String =
    Try(parser.parse(cypher)).toOption.map { parsed =>
      parsed.ast match {
        case _: ConditionalQueryWhen | _: NextStatement | _: QueryWithLocalDefinitions =>
          s"${usePrefix.trim} {\n${parsed.statement}\n}"
        case _: TopLevelBraces =>
          s"${usePrefix.trim} ${parsed.statement}"
        case query: Query =>
          spliceAt(usePrefix, parsed.statement, queryLeafOffsets(query))
        case _ =>
          usePrefix + cypher
      }
    }.getOrElse(usePrefix + cypher)

  private def queryLeafOffsets(query: Query): Seq[Int] = query match {
    case sq: SingleQuery     => sq.clauses.headOption.map(_.position.offset).toSeq
    case tlb: TopLevelBraces => Seq(tlb.position.offset)
    case u: Union            => queryLeafOffsets(u.lhs) ++ queryLeafOffsets(u.rhs)
    case other               => Seq(other.position.offset)
  }

  private def spliceAt(usePrefix: String, statement: String, offsets: Seq[Int]): String =
    offsets.distinct.sorted.reverse.foldLeft(statement) { (acc, offset) =>
      acc.substring(0, offset) + usePrefix + acc.substring(offset)
    }

  /**
   * Wraps `cypher` for the stitched execution path: `USE <constituent>` is routed in at each top-level leaf (see
   * [[route]]), so the fabric stitcher collapses the whole query into one remote fragment and any write persists.
   * Used by [[CompositeStitched]] for every step, and by the other generators for setup steps, since a
   * `CALL () { ... }`-wrapped write does not persist against the composite session db.
   */
  def wrapStitched(parser: CachingParser, cypher: String, usePrefix: String = usePrefix): String =
    peelDirectives(cypher)(route(parser, usePrefix, _))

  def peelDirectives(cypher: String)(wrapBody: String => String): String =
    Filter.dropTrailingSemicolon(cypher) match {
      case leadingCypherOptions(prefix, rest)     => prefix + peelDirectives(rest)(wrapBody)
      case leadingExplainOrProfile(keyword, rest) => keyword + wrapBody(rest)
      case stripped                               => wrapBody(stripped)
    }

  def isSingleGraphWrappable(query: ParsedQuery): Boolean = doNotContainAst[UseGraph](query)

  // The shapes `route` wraps in `USE g { ... }` (vs leaf-splicing) — their returns must be subquery-legal.
  def routeWrapsInBraces(query: ParsedQuery): Boolean = query.ast match {
    case _: ConditionalQueryWhen | _: NextStatement | _: QueryWithLocalDefinitions | _: TopLevelBraces => true
    case _                                                                                             => false
  }

  def backtick(name: String): String = "`" + name.replace("`", "``") + "`"

  /**
   * Outer `RETURN` re-projecting the query's terminal columns by name, for closing over a subquery wrap. On a
   * `RETURN *` terminal, `keepStar = false` yields `None` — the star can't be re-projected across a subquery boundary.
   */
  def outerReturn(ast: Statement, keepStar: Boolean): Option[String] = ast match {
    case q: Query if q.isReturning =>
      q.getReturns.headOption.flatMap { ret =>
        if (ret.returnItems.includeExisting)
          Option.when(keepStar)("RETURN *" + explicitItems(ret.returnItems.items, prefix = ", "))
        else Some("RETURN " + explicitItems(ret.returnItems.items, prefix = ""))
      }
    case _ => None
  }

  private def explicitItems(items: Seq[ReturnItem], prefix: String): String =
    if (items.isEmpty) "" else items.map(i => backtick(i.name)).mkString(prefix, ", ", "")
}
