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
package org.neo4j.cypher.internal.compiler.v2_0

object Foldable {
  implicit class TreeAny(val any: Any) extends AnyVal {
    def children: Iterator[Any] = any match {
      case p: Product => p.productIterator
      case s: Seq[_] => s.toIterator
      case _ => Iterator.empty
    }
  }

  implicit class FoldableAny(val any: Any) extends AnyVal {
    def fold[R](init: R)(f: PartialFunction[Any, R => R]): R = {
      val acc = if (f.isDefinedAt(any))
        f(any)(init)
      else
        init
      any.children.foldLeft(acc)((a, t) => t.fold(a)(f))
    }

    def foldt[R](init: R)(f: PartialFunction[Any, (R, R => R) => R]): R = {
      if (f.isDefinedAt(any))
        f(any)(init, any.children.foldLeft(_)((a, t) => t.foldt(a)(f)))
      else
        any.children.foldLeft(init)((a, t) => t.foldt(a)(f))
    }
  }
}

trait Foldable
