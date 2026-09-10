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
package org.neo4j.cypher.internal.expressions

import org.neo4j.cypher.internal.expressions.PatternPart.Selector
import org.neo4j.cypher.internal.expressions.functions.Category
import org.neo4j.cypher.internal.expressions.functions.FunctionWithName
import org.neo4j.cypher.internal.label_expressions.LabelExpression
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.DeprecatedFeature
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.helpers.LazyVal
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTPath

import scala.annotation.tailrec

/**
 * Represents a comma-separated list of pattern parts. Therefore, this is known in the parser as PatternList.
 * As we (in contrast to PatternParts) can use this to describe arbitrary shaped graph structures, GQL refers to these as graph patterns.
 */

sealed trait Pattern extends ASTNode {

  def patternParts: Seq[PatternPart]

  def length: Int = lazyLength.value

  private val lazyLength: LazyVal[Int] = LazyVal {
    this.folder.fold(0) {
      case RelationshipChain(_, _, _) => _ + 1
      case _                          => identity
    }
  }
}

object Pattern {

  final case class ForMatch(patternParts: Seq[PrefixedPatternPart])(val position: InputPosition) extends Pattern

  final case class ForUpdate(patternParts: Seq[NonPrefixedPatternPart])(val position: InputPosition) extends Pattern
      with HasMappableExpressions[ForUpdate] {

    override def mapExpressions(f: Expression => Expression): ForUpdate =
      copy(patternParts.map(_.mapExpressions(f)))(this.position)
  }

  sealed trait SemanticContext {
    def name: String = SemanticContext.name(this)
    def description: String = SemanticContext.description(this)
  }

  object SemanticContext {
    case object Match extends SemanticContext
    case object Merge extends SemanticContext
    case object Create extends SemanticContext
    case object Insert extends SemanticContext
    case object Expression extends SemanticContext

    def name(ctx: SemanticContext): String = ctx match {
      case Match      => "MATCH"
      case Merge      => "MERGE"
      case Create     => "CREATE"
      case Insert     => "INSERT"
      case Expression => "expression"
    }

    def description(ctx: SemanticContext): String = ctx match {
      case Match      => "a MATCH clause"
      case Merge      => "a MERGE clause"
      case Create     => "a CREATE clause"
      case Insert     => "an INSERT clause"
      case Expression => "an expression"
    }
  }
}

case class RelationshipsPattern(element: RelationshipChain)(val position: InputPosition) extends ASTNode

/**
 * Represents one part in the comma-separated list of a pattern.
 *
 * In the parser, this is just referred to as Pattern.
 * As we (in contrast to pattern lists) can only use this to describe linear graph structures, GQL refers to these as path patterns.
 */
sealed abstract class PatternPart extends ASTNode {
  def allVariables: Set[LogicalVariable]
  def element: PatternElement
  def dependencies: Set[LogicalVariable]
  def pathVariable: Option[LogicalVariable]

  def boundaryNodes: Set[LogicalVariable] = lazyBoundaryNodes.value

  def strictInteriorVariables: Set[LogicalVariable] = lazyStrictInteriorVariables.value

  private val lazyBoundaryNodes: LazyVal[Set[LogicalVariable]] =
    LazyVal(PatternElement.boundaryNodes(element))

  private val lazyStrictInteriorVariables: LazyVal[Set[LogicalVariable]] = LazyVal {
    element match {
      // QPPs connect the adjacent nodes but do not have any boundary nodes themselves.
      // Therefore, everything in a QPP is a strict interior node. Boundary nodes should be connected through a PathConcatenation
      case _: QuantifiedPath => element.allVariables
      case element           => element.allVariables.filterNot(boundaryNodes)
    }
  }

  def isBounded: Boolean
  def isFixedLength: Boolean

  def containsDynamicPattern: Boolean
}

sealed trait NonPrefixedPatternPart extends PatternPart with HasMappableExpressions[NonPrefixedPatternPart]

case class PrefixedPatternPart(selector: Selector, pathMode: PathMode, part: NonPrefixedPatternPart)
    extends PatternPart {
  override def position: InputPosition = part.position
  override def allVariables: Set[LogicalVariable] = part.allVariables
  override def pathVariable: Option[LogicalVariable] = part.pathVariable
  override def element: PatternElement = part.element
  override def isBounded: Boolean = part.isBounded
  override def isFixedLength: Boolean = part.isFixedLength
  override def dependencies: Set[LogicalVariable] = part.dependencies

  def isSelective: Boolean = selector.isSelective

  def modifyElement(f: PatternElement => PatternElement): PrefixedPatternPart = {
    def replaceInAnonymous(app: AnonymousPatternPart): AnonymousPatternPart = app match {
      case p: PathPatternPart          => p.copy(element = f(p.element))
      case s: ShortestPathsPatternPart => s.copy(element = f(s.element))(s.position)
    }

    def replaceInNonPrefixed(npp: NonPrefixedPatternPart): NonPrefixedPatternPart = npp match {
      case npp: NamedPatternPart     => npp.copy(patternPart = replaceInAnonymous(npp.patternPart))(npp.position)
      case app: AnonymousPatternPart => replaceInAnonymous(app)
    }

    copy(part = replaceInNonPrefixed(part))
  }

  def replaceElement(newElement: PatternElement): PrefixedPatternPart =
    modifyElement(_ => newElement)

  override def containsDynamicPattern: Boolean = element.containsDynamicPattern
}

object PrefixedPatternPart {

  def apply(part: NonPrefixedPatternPart)(position: InputPosition): PrefixedPatternPart =
    PrefixedPatternPart(PatternPart.AllPaths()(position), PathMode.Walk(implicitlyCreated = true)(position), part)

  def apply(selector: Selector, part: NonPrefixedPatternPart): PrefixedPatternPart =
    PrefixedPatternPart(selector, PathMode.Walk(implicitlyCreated = true)(selector.position), part)
}

case class NamedPatternPart(variable: Variable, patternPart: AnonymousPatternPart)(val position: InputPosition)
    extends NonPrefixedPatternPart {
  override def element: PatternElement = patternPart.element

  override def allVariables: Set[LogicalVariable] = patternPart.allVariables + variable

  override def pathVariable: Option[LogicalVariable] = Some(variable)

  override def isBounded: Boolean = patternPart.isBounded

  override def isFixedLength: Boolean = patternPart.isFixedLength

  override def mapExpressions(f: Expression => Expression): NonPrefixedPatternPart =
    copy(patternPart = patternPart.mapExpressions(f).asInstanceOf[AnonymousPatternPart])(this.position)

  override def dependencies: Set[LogicalVariable] = patternPart.dependencies

  override def containsDynamicPattern: Boolean = element.containsDynamicPattern
}

sealed trait AnonymousPatternPart extends NonPrefixedPatternPart {
  override def allVariables: Set[LogicalVariable] = element.allVariables
  override def containsDynamicPattern: Boolean = element.containsDynamicPattern
  override def pathVariable: Option[LogicalVariable] = None
}

case class PathPatternPart(element: PatternElement) extends AnonymousPatternPart {
  override def position: InputPosition = element.position
  override def isBounded: Boolean = element.isBounded
  override def isFixedLength: Boolean = element.isFixedLength
  override def dependencies: Set[LogicalVariable] = element.dependencies

  override def dup(children: Seq[AnyRef]): this.type = {
    PathPatternPart(children.head.asInstanceOf[PatternElement]).asInstanceOf[this.type]
  }

  override def mapExpressions(f: Expression => Expression): NonPrefixedPatternPart =
    copy(element.mapExpressions(f))

  override def containsDynamicPattern: Boolean = element.containsDynamicPattern
}

case class ShortestPathsPatternPart(element: PatternElement, single: Boolean)(val position: InputPosition)
    extends AnonymousPatternPart {

  val name: String =
    if (single)
      "shortestPath"
    else
      "allShortestPaths"

  override def isBounded: Boolean = true
  override def isFixedLength: Boolean = false

  override def mapExpressions(f: Expression => Expression): NonPrefixedPatternPart =
    copy(element.mapExpressions(f))(this.position)

  override def dependencies: Set[LogicalVariable] = element.dependencies

  override def containsDynamicPattern: Boolean = element.containsDynamicPattern
}

object ShortestPathShowInfo extends FunctionWithName {
  def name: String = "shortestPath"

  val functionInfoForShow: Seq[FunctionTypeSignature] = Vector(
    FunctionTypeSignature(
      function = this,
      outputType = CTPath,
      names = Vector("pathPattern"),
      description =
        "Returns the shortest path for a given path pattern with a variable-length relationship. If multiple shortest paths exist, one is returned non-deterministically.",
      category = Category.PATH,
      argumentTypes = Vector(CTPath),
      argumentDescriptions = Map(
        "pathPattern" -> "A path pattern containing a variable-length relationship."
      )
    )
  )
}

object AllShortestPathsShowInfo extends FunctionWithName {
  def name: String = "allShortestPaths"

  val functionInfoForShow: Seq[FunctionTypeSignature] = Vector(
    FunctionTypeSignature(
      function = this,
      outputType = CTList(CTPath),
      names = Vector("pathPattern"),
      description =
        "Returns all shortest paths for a given path pattern with a variable-length relationship.",
      category = Category.LIST,
      argumentTypes = Vector(CTPath),
      argumentDescriptions = Map(
        "pathPattern" -> "A path pattern containing a variable-length relationship."
      )
    )
  )
}

object PatternPart {

  def apply(element: PatternElement): PathPatternPart =
    PathPatternPart(element)

  sealed trait Selector extends ASTNode {
    def prettified: String = s"$prettifiedPrefix $prettifiedSuffix"
    def prettifiedPrefix: String
    def prettifiedSuffix: String

    def isSelective: Boolean
  }

  sealed trait CountedSelector extends Selector {
    val count: Either[PathLengthQuantifier, Parameter]
  }

  sealed trait SelectiveSelector extends Selector {
    override def isSelective: Boolean = true
  }

  case class AnyPath(count: Either[PathLengthQuantifier, Parameter])(val position: InputPosition)
      extends SelectiveSelector
      with CountedSelector {

    override def prettifiedPrefix: String = count match {
      case Left(n)  => s"ANY ${n.value}"
      case Right(p) => s"ANY $$${p.name}"
    }

    override def prettifiedSuffix: String = "PATHS"
  }

  case class AllPaths()(val position: InputPosition) extends Selector {
    override def prettifiedPrefix: String = "ALL"

    override def prettifiedSuffix: String = "PATHS"

    override def isSelective: Boolean = false
  }

  case class AnyShortestPath(count: Either[
    PathLengthQuantifier,
    Parameter
  ])(val position: InputPosition)
      extends SelectiveSelector
      with CountedSelector {

    override def prettifiedPrefix: String = count match {
      case Left(n)  => s"SHORTEST ${n.value}"
      case Right(p) => s"SHORTEST $$${p.name}"
    }

    override def prettifiedSuffix: String = "PATHS"
  }

  case class AllShortestPaths()(val position: InputPosition) extends SelectiveSelector {
    override def prettifiedPrefix: String = "ALL SHORTEST"

    override def prettifiedSuffix: String = "PATHS"
  }

  case class ShortestGroups(count: Either[
    PathLengthQuantifier,
    Parameter
  ])(val position: InputPosition)
      extends SelectiveSelector
      with CountedSelector {

    override def prettifiedPrefix: String = count match {
      case Left(n)  => s"SHORTEST ${n.value}"
      case Right(p) => s"SHORTEST $$${p.name}"
    }

    override def prettifiedSuffix: String = "PATH GROUPS"
  }
}

/**
 * Contains a list of elements that are concatenated in the query.
 *
 * NOTE that the concatenation is recorded only in the order of the factors in the sequence.
 * That is that `factors(i)` is concatenated with `factors(i - 1)` and `factors(i + 1)` if they exist.
 */
case class PathConcatenation(factors: Seq[PathFactor])(val position: InputPosition) extends PatternElement {
  override def allVariables: Set[LogicalVariable] = factors.view.flatMap(_.allVariables).toSet

  override def allSingletonNodeVariables: Set[LogicalVariable] = factors.view.flatMap(_.allSingletonNodeVariables).toSet

  override def allSingletonVariablesLeftToRight: Seq[LogicalVariable] =
    factors.flatMap(_.allSingletonVariablesLeftToRight)

  override def variable: Option[LogicalVariable] = None

  override def isBounded: Boolean = factors.forall(_.isBounded)

  override def isFixedLength: Boolean = factors.forall(_.isFixedLength)

  override def mapExpressions(f: Expression => Expression): PatternElement =
    copy(factors.map(_.mapExpressions(f)).asInstanceOf[Seq[PathFactor]])(this.position)

  override def dependencies: Set[LogicalVariable] = factors.view.flatMap(_.dependencies).toSet

  override def containsDynamicPattern: Boolean = factors.exists(_.containsDynamicPattern)
}

/**
 * Elements of this trait can be put next to each other to form a juxtaposition in a pattern.
 */
sealed trait PathFactor extends PatternElement

sealed trait PatternAtom extends ASTNode

case class QuantifiedPath(
  part: NonPrefixedPatternPart,
  quantifier: GraphPatternQuantifier,
  optionalWhereExpression: Option[Expression],
  variableGroupings: Set[VariableGrouping]
)(val position: InputPosition)
    extends PathFactor with PatternAtom {

  override def allVariables: Set[LogicalVariable] = variableGroupings.map(_.group)

  override def allSingletonNodeVariables: Set[LogicalVariable] = Set.empty

  override def allSingletonVariablesLeftToRight: Seq[LogicalVariable] = Seq.empty

  override def variable: Option[LogicalVariable] = None

  override def isBounded: Boolean = quantifier match {
    case FixedQuantifier(_)           => true
    case IntervalQuantifier(_, upper) => upper.nonEmpty
    case PlusQuantifier()             => false
    case StarQuantifier()             => false
  }

  override def isFixedLength: Boolean = quantifier match {
    case _: FixedQuantifier                                                         => true
    case IntervalQuantifier(Some(lower), Some(upper)) if lower.value == upper.value => true
    case _                                                                          => false
  }

  override def mapExpressions(f: Expression => Expression): PatternElement = copy(
    part.mapExpressions(f),
    quantifier.mapExpressions(f),
    optionalWhereExpression.map(f),
    variableGroupings.map(_.mapExpressions(f))
  )(this.position)

  override def dependencies: Set[LogicalVariable] = part.dependencies ++
    optionalWhereExpression.toSet[Expression].flatMap(_.dependencies)

  override def containsDynamicPattern: Boolean = optionalWhereExpression.exists(_.containsDynamicExpression) ||
    part.containsDynamicPattern
}

object QuantifiedPath {

  def apply(
    part: NonPrefixedPatternPart,
    quantifier: GraphPatternQuantifier,
    optionalWhereExpression: Option[Expression]
  )(position: InputPosition): QuantifiedPath = {
    val entityBindings = part.allVariables.map(getGrouping(_, position))
    QuantifiedPath(part, quantifier, optionalWhereExpression, entityBindings)(position)
  }

  def getGrouping(innerVar: LogicalVariable, qppPosition: InputPosition): VariableGrouping = {
    VariableGrouping(innerVar.copyId, innerVar.withPosition(qppPosition))(qppPosition)
  }
}

case class AllReduceAccumulator(
  initial: Expression,
  previous: LogicalVariable,
  next: LogicalVariable
)(val position: InputPosition)
    extends ASTNode with HasMappableExpressions[AllReduceAccumulator] {

  override def toString: String = s"(initial=$initial, previous=${previous.name}, next=${next.name})"

  override def mapExpressions(f: Expression => Expression): AllReduceAccumulator = copy(
    f(initial),
    f(previous).asInstanceOf[LogicalVariable],
    f(next).asInstanceOf[LogicalVariable]
  )(this.position)
}

/**
 * Describes a variable that is exposed from a [[QuantifiedPath]].
 *
 * @param singleton the singleton variable inside the QuantifiedPath.
 * @param group the group variable exposed outside of the QuantifiedPath.
 */
case class VariableGrouping(singleton: LogicalVariable, group: LogicalVariable)(val position: InputPosition)
    extends ASTNode with HasMappableExpressions[VariableGrouping] {

  override def toString: String = s"(singletonName=${singleton.name}, groupName=${group.name})"

  override def mapExpressions(f: Expression => Expression): VariableGrouping = copy(
    f(singleton).asInstanceOf[LogicalVariable],
    f(group).asInstanceOf[LogicalVariable]
  )(this.position)
}

object VariableGrouping {

  def singletonToGroup(groupings: Set[VariableGrouping], singletonName: LogicalVariable): Option[LogicalVariable] = {
    groupings.collectFirst {
      case VariableGrouping(`singletonName`, groupName) => groupName
    }
  }
}

// We can currently parse these but not plan them. Therefore, we represent them in the AST but disallow them in semantic checking when concatenated and unwrap them otherwise.
case class ParenthesizedPath(
  part: NonPrefixedPatternPart,
  optionalWhereClause: Option[Expression]
)(val position: InputPosition)
    extends PathFactor with PatternAtom {

  override def allVariables: Set[LogicalVariable] = part.element.allVariables

  override def allSingletonNodeVariables: Set[LogicalVariable] = part.element.allSingletonNodeVariables

  override def allSingletonVariablesLeftToRight: Seq[LogicalVariable] = part.element.allSingletonVariablesLeftToRight

  override def variable: Option[LogicalVariable] = None

  override def mapExpressions(f: Expression => Expression): PatternElement =
    copy(part.mapExpressions(f), optionalWhereClause.map(f))(this.position)

  override def dependencies: Set[LogicalVariable] = part.dependencies ++
    optionalWhereClause.toSet[Expression].flatMap(_.dependencies)

  override def isBounded: Boolean = part.isBounded

  override def isFixedLength: Boolean = part.isFixedLength

  override def containsDynamicPattern: Boolean = optionalWhereClause.exists(_.containsDynamicExpression) ||
    part.containsDynamicPattern
}

object ParenthesizedPath {

  def apply(part: NonPrefixedPatternPart)(position: InputPosition): ParenthesizedPath =
    ParenthesizedPath(part, None)(position)
}

sealed abstract class PatternElement extends ASTNode with HasMappableExpressions[PatternElement] {
  def allVariables: Set[LogicalVariable]

  // TODO Set may not be sufficient when we need to identify boundary nodes for the SIMPLE path mode
  //  See PLAN-2343
  def allSingletonNodeVariables: Set[LogicalVariable]

  /**
   * In contrast to allVariables, this does not return variables that are nested inside QPPs.
   */
  def allSingletonVariablesLeftToRight: Seq[LogicalVariable]
  def variable: Option[LogicalVariable]
  def isBounded: Boolean
  def isFixedLength: Boolean
  def dependencies: Set[LogicalVariable]
  def containsDynamicPattern: Boolean

  def isSingleNode = false
}

object PatternElement {

  /**
   * Returns the boundary nodes of this pattern element.
   *
   * In well-formed AST shapes encountered after [[QuantifiedPathPatternNodeInsertRewriter]] has run,
   * QPPs at the boundary of a [[PathConcatenation]] have been padded with filler nodes, so the head
   * and last factors are [[SimplePattern]]s. During semantic analysis the AST is not yet padded, so
   * we may encounter a QPP at the boundary; in that case we recurse into the QPP's inner element and
   * use its boundary singleton (which becomes the boundary node post-padding).
   */
  @tailrec
  def boundaryNodes(element: PatternElement): Set[LogicalVariable] = {
    element match {
      case pattern: SimplePattern =>
        val allVars = pattern.allSingletonVariablesLeftToRight
        Set.empty ++ allVars.headOption ++ allVars.lastOption
      case PathConcatenation(factors) =>
        Set.empty ++ leftBoundaryNode(factors.head) ++ rightBoundaryNode(factors.last)
      case ParenthesizedPath(part, _) => boundaryNodes(part.element)
      case _                          => throw new IllegalStateException()
    }
  }

  @tailrec
  private def leftBoundaryNode(element: PatternElement): Option[LogicalVariable] = element match {
    case sp: SimplePattern          => sp.allSingletonVariablesLeftToRight.headOption
    case qp: QuantifiedPath         => leftBoundaryNode(qp.part.element)
    case ParenthesizedPath(part, _) => leftBoundaryNode(part.element)
    case PathConcatenation(factors) => leftBoundaryNode(factors.head)
  }

  @tailrec
  private def rightBoundaryNode(element: PatternElement): Option[LogicalVariable] = element match {
    case sp: SimplePattern          => sp.allSingletonVariablesLeftToRight.lastOption
    case qp: QuantifiedPath         => rightBoundaryNode(qp.part.element)
    case ParenthesizedPath(part, _) => rightBoundaryNode(part.element)
    case PathConcatenation(factors) => rightBoundaryNode(factors.last)
  }
}

/**
 * A part of the pattern that consists of alternating nodes and relationships, starting and ending in a node.
 */
sealed abstract class SimplePattern extends PathFactor {
  def allSingletonVariablesLeftToRight: Seq[LogicalVariable]
}

case class RelationshipChain(
  element: SimplePattern,
  relationship: RelationshipPattern,
  rightNode: NodePattern
)(val position: InputPosition)
    extends SimplePattern {

  override def variable: Option[LogicalVariable] = relationship.variable

  override def allVariables: Set[LogicalVariable] = element.allVariables ++ relationship.variable ++ rightNode.variable

  override def allSingletonNodeVariables: Set[LogicalVariable] = element.allSingletonNodeVariables ++ rightNode.variable

  override def allSingletonVariablesLeftToRight: Seq[LogicalVariable] =
    element.allSingletonVariablesLeftToRight ++ relationship.variable ++ rightNode.variable

  override def isBounded: Boolean = relationship.isBounded && element.isBounded

  override def isFixedLength: Boolean = relationship.isSingleLength && element.isFixedLength && rightNode.isFixedLength

  override def dependencies: Set[LogicalVariable] =
    element.dependencies ++ relationship.dependencies ++ rightNode.dependencies

  @tailrec
  final def leftNode: NodePattern = element match {
    case node: NodePattern      => node
    case rel: RelationshipChain => rel.leftNode
  }

  override def mapExpressions(f: Expression => Expression): PatternElement = copy(
    element.mapExpressions(f).asInstanceOf[SimplePattern],
    relationship.mapExpressions(f),
    rightNode.mapExpressions(f).asInstanceOf[NodePattern]
  )(this.position)

  override def containsDynamicPattern: Boolean = element.containsDynamicPattern ||
    relationship.labelExpression.exists(_.containsDynamicLabelOrTypeExpression) ||
    rightNode.containsDynamicPattern
}

/**
 * Represents one node in a pattern.
 */
case class NodePattern(
  variable: Option[LogicalVariable],
  labelExpression: Option[LabelExpression],
  properties: Option[Expression],
  predicate: Option[Expression]
)(val position: InputPosition)
    extends SimplePattern with PatternAtom {

  override def allVariables: Set[LogicalVariable] = variable.toSet

  override def allSingletonNodeVariables: Set[LogicalVariable] = variable.toSet

  override def allSingletonVariablesLeftToRight: Seq[LogicalVariable] = variable.toSeq

  override def isSingleNode = true

  override def isBounded: Boolean = true

  override def isFixedLength: Boolean = true

  override def mapExpressions(f: Expression => Expression): PatternElement = {
    // The parser only allows parameters and Map literals in the properties position of a pattern.
    // The AST allows arbitrary expressions, however.
    // In order to make sure that queries can still be prettified to parseable Cypher, even after using
    // mapExpressions, we try to rewrite the items inside a MapExpression, instead of the whole MapExpression.
    val mappedProperties = properties.map {
      case me: MapExpression => me.mapExpressions(f)
      case x                 => f(x)
    }

    copy(
      variable = variable.map(f).asInstanceOf[Option[LogicalVariable]],
      properties = mappedProperties,
      labelExpression = labelExpression.map(_.mapExpressions(f)),
      predicate = predicate.map(f)
    )(this.position)
  }

  override def dependencies: Set[LogicalVariable] = (properties.toSet ++ predicate).flatMap(_.dependencies)

  override def containsDynamicPattern: Boolean = labelExpression.exists(_.containsDynamicLabelOrTypeExpression)

}

object NodePattern {
  // (where {...})
  //  ^
  case object WhereVariableInNodePatterns extends DeprecatedFeature.DeprecatedIn5ErrorIn25
}

/**
 * Represents one relationship (without its neighbouring nodes) in a pattern.
 */
case class RelationshipPattern(
  variable: Option[LogicalVariable],
  labelExpression: Option[LabelExpression],
  length: Option[Option[Range]],
  properties: Option[Expression],
  predicate: Option[Expression],
  direction: SemanticDirection
)(val position: InputPosition) extends ASTNode with PatternAtom with HasMappableExpressions[RelationshipPattern] {

  def isSingleLength: Boolean = length.isEmpty

  def isDirected: Boolean = direction != SemanticDirection.BOTH

  def isBounded: Boolean = length match {
    case Some(Some(Range(_, Some(_)))) => true
    case None                          => true
    case _                             => false
  }

  def dependencies: Set[LogicalVariable] =
    (predicate.toSet ++ properties).flatMap(_.dependencies)

  override def mapExpressions(f: Expression => Expression): RelationshipPattern = {
    // The parser only allows parameters and Map literals in the properties position of a pattern.
    // The AST allows arbitrary expressions, however.
    // In order to make sure that queries can still be prettified to parseable Cypher, even after using
    // mapExpressions, we try to rewrite the items inside a MapExpression, instead of the whole MapExpression.
    val mappedProperties = properties.map {
      case me: MapExpression => me.mapExpressions(f)
      case x                 => f(x)
    }

    copy(
      variable = variable.map(f).asInstanceOf[Option[LogicalVariable]],
      length = length.map(_.map(_.mapExpressions(f))),
      labelExpression = labelExpression.map(_.mapExpressions(f)),
      properties = mappedProperties,
      predicate = predicate.map(f)
    )(this.position)
  }
}

object RelationshipPattern {
  // -[where {...}]-
  //   ^
  case object WhereVariableInRelationshipPatterns extends DeprecatedFeature.DeprecatedIn5ErrorIn25
}
