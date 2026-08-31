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
package org.neo4j.cypher.internal.preparser

import org.antlr.v4.runtime.Token
import org.antlr.v4.runtime.tree.TerminalNode
import org.neo4j.cypher.internal.parser.lexer.CypherToken
import org.neo4j.cypher.internal.util.InputPosition

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class StatefulPreparserListener extends CypherPreparserBaseListener {

  val settings: mutable.ArrayBuffer[PreParserOption] = ArrayBuffer.empty[PreParserOption]
  var queryPosition: Option[InputPosition] = None

  override def exitPreparserOptions(
    ctx: CypherPreparserParser.PreparserOptionsContext
  ): Unit = {}

  override def exitOption(ctx: CypherPreparserParser.OptionContext): Unit = {
    if (ctx.EXPLAIN() != null) settings.addOne(PreParserOption.explain(pos(ctx.EXPLAIN())))
    if (ctx.PROFILE() != null) settings.addOne(PreParserOption.profile(pos(ctx.PROFILE())))
    if (ctx.VERSION() != null) settings.addOne(PreParserOption.version(ctx.VERSION().getText, pos(ctx.VERSION())))
  }

  override def exitPlanMode(ctx: CypherPreparserParser.PlanModeContext): Unit = {
    if (ctx.SCOPE() != null) settings.addOne(PreParserOption.scope(pos(ctx.SCOPE())))
    if (ctx.PLAN() != null) settings.addOne(PreParserOption.plan(pos(ctx.PLAN())))
    // PLAN we just swallow
  }

  override def exitSetting(ctx: CypherPreparserParser.SettingContext): Unit = {
    if (ctx.IDENTIFIER().size() == 2) {
      settings.addOne(PreParserOption.generic(
        ctx.IDENTIFIER(0).getText,
        ctx.IDENTIFIER(1).getText,
        pos(ctx.IDENTIFIER(0))
      ))
    } else if (ctx.IDENTIFIER().size() == 1 && ctx.VERSION() != null) {
      settings.addOne(PreParserOption.generic(
        ctx.IDENTIFIER(0).getText,
        ctx.VERSION().getText,
        pos(ctx.IDENTIFIER(0))
      ))
    }
  }

  override def exitStatement(ctx: CypherPreparserParser.StatementContext): Unit = {
    queryPosition = Some(pos(ctx.getStart))
  }

  def pos(node: TerminalNode): InputPosition = {
    pos(node.getSymbol)
  }

  def pos(t: Token): InputPosition = {
    t.asInstanceOf[CypherToken].position()
  }
}
