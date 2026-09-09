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

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.FreeProjection
import org.neo4j.cypher.internal.ast.ParsedAsYield
import org.neo4j.cypher.internal.ast.ProjectionClause
import org.neo4j.cypher.internal.ast.ReturnItem
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.ScopeClauseSubqueryCall
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.ast.semantics.scoping.ScopeState
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.StatementRewriter
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.factories.ParsePipelineTransformerFactory
import org.neo4j.cypher.internal.frontend.phases.factories.ParsingConfig
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.IsolateSubqueriesInMutatingPatterns.SubqueriesInMutatingPatternsIsolated
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.UpToDateScopes
import org.neo4j.cypher.internal.rewriting.conditions.ContainsNoStarProjections
import org.neo4j.cypher.internal.rewriting.conditions.ProjectionClausesHaveSemanticInfo
import org.neo4j.cypher.internal.rewriting.rewriters.computeDependenciesForExpressions.ExpressionsHaveComputedDependencies
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.Condition
import org.neo4j.cypher.internal.util.topDown

/**
 * Materialises every `*` in the statement from the recorded scopes: the return items a `WITH *`,
 * `RETURN *` or `YIELD *` stands for, and the imported variables of a `CALL (*)`.
 *
 * This runs ahead of [[ExpandClauses]] because a star is expanded from the scope recorded for the clause
 * it appears in, and the NEXT / WHEN / top-level-braces expansions rewrite exactly those clauses.
 */
case object ExpandStarProjections extends StatementRewriter with StepSequencer.Step
    with ParsePipelineTransformerFactory {

  private def containsStar(statement: ASTNode): Boolean =
    statement.folder.treeExists {
      case ri: ReturnItems if ri.includeExisting            => true
      case sq: ScopeClauseSubqueryCall if sq.isImportingAll => true
    }

  override def process(from: BaseState, context: BaseContext): BaseState =
    if (containsStar(from.statement())) super.process(from, context)
    else from

  override def instance(from: BaseState, context: BaseContext): Rewriter =
    getRewriter(from.scopeState(), from.anonymousVariableNameGenerator, context.cypherVersion)

  def getRewriter(
    scopeState: ScopeState,
    anonVarNameGen: AnonymousVariableNameGenerator,
    version: CypherVersion
  ): Rewriter = version match {
    case CypherVersion.Cypher5 => expandStars(scopeState, version)
    // A star can expand to nothing, and whether it did is only known once it has been expanded.
    case CypherVersion.Cypher25 =>
      expandStars(scopeState, version) andThen DropEmptyProjections.rewriter(anonVarNameGen)
  }

  private def expandStars(scopeState: ScopeState, version: CypherVersion): Rewriter = topDown(Rewriter.lift {
    case w: With if w.returnItems.includeExisting && isSimplifiableEmptyWith(w, scopeState, version) =>
      w.copyProjection(returnItems = ReturnItems(FreeProjection, Seq.empty, None)(w.returnItems.position))
        .withRewrittenType

    case p: ProjectionClause if p.returnItems.includeExisting =>
      val returnItems = p.returnItems
      val expandedItems = starItems(p, scopeState, version)
        .filterNot(item => returnItems.items.exists(_.name == item.name))
      p.copyProjection(returnItems =
        returnItems.copy(
          FreeProjection,
          sortExpandedItems(expandedItems, returnItems) ++ returnItems.items,
          defaultOrderOnColumns = None
        )(returnItems.position)
      )

    case call: ScopeClauseSubqueryCall if call.isImportingAll =>
      call.copy(isImportingAll = false, importedVariables = importsOf(scopeState, call))(call.position)
  })

  /**
   * A Cypher 25 WITH carries constants along with the variables, but only those the query goes on to use;
   * an importing WITH carries all of them, since it decides what the query it leads can see at all.
   */
  private def starItems(clause: ProjectionClause, scopeState: ScopeState, version: CypherVersion): Seq[ReturnItem] =
    clause match {
      case w: With
        if version != CypherVersion.Cypher5 && !w.isAggregating && w.withType != ParsedAsYield =>
        val usedNames = scopeState.referencedAnywhereInQuery(w).map(_.name)
        scopeState.getOutgoingVariablesAndConstantsReturnItemSeq(w).filter(item => usedNames.contains(item.name))
      case w: With if scopeState.isImportingWith(w) =>
        scopeState.getOutgoingVariablesAndConstantsReturnItemSeq(w)
      case other =>
        scopeState.getOutgoingVariableReturnItemSeq(other)
    }

  private def sortExpandedItems(expandedItems: Seq[ReturnItem], returnItems: ReturnItems): Seq[ReturnItem] =
    returnItems.defaultOrderOnColumns
      .map(order => expandedItems.sortBy(item => order.indexOf(item.name)))
      .getOrElse(expandedItems.sortBy(_.name))

  private def importsOf(scopeState: ScopeState, call: ScopeClauseSubqueryCall): Seq[LogicalVariable] =
    scopeState.getReferenced(call).toSeq.sortBy(_.name).map(_.copyId)

  /**
   * An importing WITH is never simplifiable: it decides what the query it leads can see.
   */
  private def isSimplifiableEmptyWith(clause: Clause, scopeState: ScopeState, version: CypherVersion): Boolean =
    clause match {
      case w @ With(false, ReturnItems(_, Seq(), _), None, None, None, None, None, withType)
        if version != CypherVersion.Cypher5 &&
          withType != ParsedAsYield &&
          !scopeState.isImportingWith(w) => true
      case _ => false
    }

  override def preConditions: Set[Condition] =
    Set(
      UpToDateScopes,
      SemanticTypeCheckCompleted,
      DeprecatedSemanticsReplaced,
      SubqueriesInMutatingPatternsIsolated
    )

  override def postConditions: Set[Condition] = Set(ContainsNoStarProjections)

  override def invalidatedConditions: Set[Condition] =
    Set(ProjectionClausesHaveSemanticInfo, UpToDateScopes, ExpressionsHaveComputedDependencies)

  override def getTransformer(config: ParsingConfig): Transformer[BaseContext, BaseState, BaseState] =
    ExpandStarProjections

}
