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
package org.neo4j.cypher.internal.ast.semantics.scoping

import org.neo4j.cypher.internal.ast.ASTAnnotationMap.PositionedNode
import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.ReturnItem
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.ast.semantics.scoping.ScopeState.RecordedScopes
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.Ref
import org.neo4j.cypher.internal.util.helpers.LazyVal

import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap

case class ScopeState(
  workingScope: WorkingScope,
  recordedScopes: RecordedScopes,
  explainScope: Option[WorkingScope] = None
) {

  private val recordedScopesByStructureLazy: LazyVal[Map[PositionedNode[ASTNode], WorkingScope]] =
    LazyVal(recordedScopes.iterator.map { case (ref, scope) => PositionedNode(ref.value) -> scope }.toMap)

  private def recordedScopesByStructure: Map[PositionedNode[ASTNode], WorkingScope] =
    recordedScopesByStructureLazy.value

  private def scopeOf(ast: ASTNode): WorkingScope =
    recordedScopes.getOrElse(Ref(ast), recordedScopesByStructure(PositionedNode(ast)))

  def scopeOfOpt(ast: ASTNode): Option[WorkingScope] =
    recordedScopes.get(Ref(ast)).orElse(recordedScopesByStructure.get(PositionedNode(ast)))

  private val parentScopesLazy: LazyVal[Map[Ref[ASTNode], WorkingScope]] =
    LazyVal {
      val byRef = Map.newBuilder[Ref[ASTNode], WorkingScope]
      def index(scope: WorkingScope): Unit = scope.children.foreach { child =>
        byRef += Ref(child.astNode) -> scope
        index(child)
      }
      index(workingScope)
      byRef.result()
    }

  private val parentScopesByStructureLazy: LazyVal[Map[PositionedNode[ASTNode], WorkingScope]] =
    LazyVal(parentScopesLazy.value.map { case (child, parent) => PositionedNode(child.value) -> parent })

  private def parentScopeOf(ast: ASTNode): Option[WorkingScope] =
    parentScopesLazy.value.get(Ref(ast))
      .orElse(parentScopesByStructureLazy.value.get(PositionedNode(ast)))

  /**
   * The nearest ancestor scope recorded for a [[Query]]. Empty when `ast` has no recorded scope to walk up
   * from, as for the YIELD of a command clause: the survey records a synthesised replacement of that clause
   * rather than the node standing in the tree.
   */
  @tailrec
  final def enclosingQueryScope(ast: ASTNode): Option[WorkingScope] = parentScopeOf(ast) match {
    case Some(parent) if parent.astNode.isInstanceOf[Query] => Some(parent)
    case Some(parent)                                       => enclosingQueryScope(parent.astNode)
    case None                                               => None
  }

  /**
   * Keyed on the query scope, so every star in the same query shares one entry rather than walking the same
   * scope subtree once per star. A race can compute the same set twice, which is harmless: the value is
   * derived from an immutable tree, so both results are equal.
   */
  private val referencedInQueryCache: TrieMap[Ref[WorkingScope], Set[LogicalVariable]] = TrieMap.empty

  /**
   * Every variable used anywhere in the innermost query containing `ast`, nested queries included.
   * Empty when there is no such query scope, see [[enclosingQueryScope]].
   */
  def referencedAnywhereInQuery(ast: ASTNode): Set[LogicalVariable] =
    enclosingQueryScope(ast)
      .map(queryScope => referencedInQueryCache.getOrElseUpdate(Ref(queryScope), queryScope.allReferencedSymbols))
      .getOrElse(Set.empty)

  def isImportingWith(clause: With): Boolean =
    enclosingQueryScope(clause).exists(scope =>
      scope.inImportingWith && (scope.astNode match {
        case sq: SingleQuery =>
          sq.partitionedClauses.importingWith.exists(w => PositionedNode(w) == PositionedNode(clause))
        case _ => false
      })
    )

  def getOutgoing(ast: ASTNode): Seq[LogicalVariable] = scopeOf(ast).outgoing.variables.map(_.copyId).toSeq

  def getOutgoingConstantsAndVariables(ast: ASTNode): Seq[LogicalVariable] =
    scopeOf(ast).outgoing.allSymbols.map(_.copyId).toSeq

  def getReferenced(ast: ASTNode): Set[LogicalVariable] =
    scopeOf(ast).referenced.getVariables.map(_.copyId).toSet

  def getReferenced(ast: ASTNode, default: Set[LogicalVariable]): Set[LogicalVariable] =
    scopeOfOpt(ast).map(_.referenced.getVariables.map(_.copyId).toSet).getOrElse(default)

  /**
   * The declarations that `ast`'s references resolve to (empty when `ast` has no recorded scope) are
   * used by the GROUP BY subclause substitution to detect when an expression is shadowed by a
   * projection alias.
   */
  def referenceTargets(ast: ASTNode): Set[LogicalVariable] =
    scopeOfOpt(ast).map(_.referenced.references.values.map(_.value).toSet).getOrElse(Set.empty)

  def getResultCols(ast: ASTNode): Seq[LogicalVariable] = scopeOf(ast).result match {
    case TableResult(cols) => cols
    case _                 => Seq.empty
  }

  def getIncomingConstants(ast: ASTNode): Seq[LogicalVariable] =
    scopeOf(ast).incoming match {
      case rc: RegularContext => rc.constants.toSeq
      case wc                 => wc.allSymbols.toSeq
    }

  def getIncomingVariables(ast: ASTNode): Seq[LogicalVariable] =
    scopeOf(ast).incoming match {
      case rc: RegularContext => rc.variables.toSeq
      case wc                 => wc.allSymbols.toSeq
    }

  def getAllIncoming(ast: ASTNode): Set[LogicalVariable] = scopeOf(ast).incoming.allSymbols

  def getResult(ast: ASTNode): Result = scopeOf(ast).result

  def getOutgoingVariableReturnItemSeq(ast: ASTNode): Seq[ReturnItem] =
    getOutgoing(ast).map(v =>
      AliasedReturnItem(v.withPosition(ast.position), v.withPosition(ast.position))(ast.position)
    )

  def getOutgoingVariablesAndConstantsReturnItemSeq(ast: ASTNode): Seq[ReturnItem] =
    getOutgoingConstantsAndVariables(ast).map(v =>
      AliasedReturnItem(v.withPosition(ast.position), v.withPosition(ast.position))(ast.position)
    )

}

object ScopeState {

  type RecordedScopes = Map[Ref[ASTNode], WorkingScope]

  def emptyRecordedScopes: RecordedScopes = Map.empty
}
