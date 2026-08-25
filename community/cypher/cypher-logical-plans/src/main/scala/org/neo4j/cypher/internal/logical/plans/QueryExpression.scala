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
package org.neo4j.cypher.internal.logical.plans

/* Interface */

sealed trait QueryExpression[+T] {
  def expression: T

  def expressions: Seq[T] = Seq(expression)

  def map[R](f: T => R): QueryExpression[R]

  def exact: Boolean = false
}

sealed trait NoArgumentQueryExpression extends QueryExpression[Nothing] {
  def expression: Nothing = throw new NotImplementedError("expression not supplied for NoArgumentQueryExpression")

  override def expressions: Seq[Nothing] = Seq.empty

  def map[R](f: Nothing => R): NoArgumentQueryExpression = this
}

/* Implementations */

case class SingleQueryExpression[T](expression: T) extends QueryExpression[T] {
  def map[R](f: T => R): SingleQueryExpression[R] = SingleQueryExpression(f(expression))

  override def exact: Boolean = true
}

case class ManyQueryExpression[T](expression: T) extends QueryExpression[T] {
  def map[R](f: T => R): ManyQueryExpression[R] = ManyQueryExpression(f(expression))

  override def exact: Boolean = true
}

case class RangeQueryExpression[T](expression: T) extends QueryExpression[T] {
  def map[R](f: T => R): RangeQueryExpression[R] = RangeQueryExpression(f(expression))
}

case class CompositeQueryExpression[T](inner: Seq[QueryExpression[T]]) extends QueryExpression[T] {
  def expression: T = throw new NotImplementedError("expression not supplied for CompositeQueryExpression")

  override def expressions: Seq[T] = inner.flatMap(_.expressions)

  def map[R](f: T => R): CompositeQueryExpression[R] = CompositeQueryExpression(inner.map(_.map(f)))

  override def exact: Boolean = inner.forall(_.exact)
}

sealed trait EntityFilterQueryExpression[+T] extends QueryExpression[T] {
  def map[R](f: T => R): EntityFilterQueryExpression[R]
}

case class MatchEntitySetQueryExpression[T](expression: T) extends EntityFilterQueryExpression[T] {
  def map[R](f: T => R): MatchEntitySetQueryExpression[R] = MatchEntitySetQueryExpression(f(expression))
}

case class PreparedEntityFilterExpression[T](expression: T) extends EntityFilterQueryExpression[T] {
  def map[R](f: T => R): PreparedEntityFilterExpression[R] = PreparedEntityFilterExpression(f(expression))
}

case object MatchAllQueryExpression extends EntityFilterQueryExpression[Nothing] {
  def expression: Nothing = throw new NotImplementedError("expression not supplied for NoArgumentQueryExpression")

  override def expressions: Seq[Nothing] = Seq.empty

  def map[R](f: Nothing => R): EntityFilterQueryExpression[R] = this
}

/* Marker implementations */

case object ExistenceQueryExpression extends NoArgumentQueryExpression

case object NonExistenceQueryExpression extends NoArgumentQueryExpression

case object AllQueryExpression extends NoArgumentQueryExpression
