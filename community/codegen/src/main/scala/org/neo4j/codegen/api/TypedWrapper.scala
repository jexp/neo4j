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
package org.neo4j.codegen.api

import org.neo4j.codegen.TypeReference
import org.neo4j.codegen.api.IntermediateRepresentation.and
import org.neo4j.codegen.api.IntermediateRepresentation.greaterThan
import org.neo4j.codegen.api.IntermediateRepresentation.greaterThanOrEqual
import org.neo4j.codegen.api.IntermediateRepresentation.lessThan
import org.neo4j.codegen.api.IntermediateRepresentation.lessThanOrEqual
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.or
import org.neo4j.codegen.api.IntermediateRepresentation.typeRefOf
import org.neo4j.codegen.api.TypeList.Supply
import org.neo4j.codegen.api.TypedIR.$arrayLength
import org.neo4j.codegen.api.TypedIR.$arrayLoad
import org.neo4j.codegen.api.TypedIR.$arraySet
import org.neo4j.codegen.api.TypedIR.$assign
import org.neo4j.codegen.api.TypedIR.$setField
import org.neo4j.codegen.api.TypedIR.$typeRef
import org.neo4j.codegen.api.TypedWrapper.$IR
import org.neo4j.codegen.api.TypedWrapper.$Ref
import org.neo4j.codegen.api.TypedWrapper.TypedWrapperConversion

import scala.annotation.implicitNotFound
import scala.annotation.unused
import scala.language.implicitConversions

/**
 * The goal of this wrapper is to make it easier to use static typing where it is useful, but gracefully degrade to
 * dynamic typing for existing code and areas where it does not make sense.
 *
 * The [[TYPE]] parameter exists only at compile-time, and the [[TypedWrapper.convertToInner]] implicit function means
 * that a TypedWrapper[_, A] can always be used in place of A.
 *
 * The inverse operation - to wrap a dynamic-typed value in this statically-typed wrapper - is provided by the
 * [[TypedWrapperConversion.typed]] method. This is explicit because the programmer should be more aware that they are
 * entering the land of static types, and that a particular type must be chosen.
 *
 * The naming convention for wrapped types is to prefix with a `$`. This is a quick indicator to the reader that they
 * are looking at statically-typed template-time code.
 */
case class TypedWrapper[+TYPE, +INNER](inner: INNER)

object TypedWrapper {

  /** Allows us to pass a statically typed wrapped value where a dynamically typed value is expected */
  implicit def convertToInner[TYPE, INNER](wrapper: TypedWrapper[TYPE, INNER]): INNER = wrapper.inner

  /**
   * Typeclass for converting any wrapped type (field, variable) to typed IR; this lets us use all the operators and
   * methods defined for IR on fields and variables too, without having to explicitly wrap everything in Load()
   */
  trait LoadAsIR[INNER, TYPE] {
    def load(wrapper: TypedWrapper[TYPE, INNER]): $IR[TYPE]
  }

  /** Typeclass instance for loading fields */
  implicit def loadFieldAsIR[TYPE]: LoadAsIR[Field, TYPE] = $field => loadField($field.inner).typed

  /** Typeclass instance for loading variables */
  implicit def loadVarAsIR[TYPE]: LoadAsIR[LocalVariable, TYPE] = $var => Load($var.name, $var.typ).typed

  /** Identity Typeclass instance for referencing IR directly */
  implicit def loadIRAsIR[TYPE]: LoadAsIR[IntermediateRepresentation, TYPE] = $var => $var

  /** Helper for an If chain with no final else clause */
  implicit def ifToIR(ifChain: IfChain): $IR[Unit] = ifChain.toIR

  /**
   * Any INNER type with a LoadAsIR implementation can be implicitly converted to IR - this means we can pass field
   * and variable references around without having to explicitly wrap in load()
   */
  implicit def autoLoad[TYPE, INNER](wrapper: TypedWrapper[TYPE, INNER])(implicit
    loadable: LoadAsIR[INNER, TYPE]): $IR[TYPE] =
    loadable.load(wrapper)

  /**
   * This extension class provides custom operators on IR. It uses the LoadAsIR typeclass to also provide those
   * operators to fields and variables with the requisite type
   */
  implicit class LoadableTypeWrapperExt[TYPE, INNER](wrapper: TypedWrapper[TYPE, INNER])(implicit
    loadable: LoadAsIR[INNER, TYPE]) {

    private def ir: $IR[TYPE] = loadable.load(wrapper)

    /**
     * Generate an instance method invocation of the given method on this object with the given parameter IR.
     * Assumes that the method is a public instance method.
     */
    def invoke[RETURN, PARAMS, IRTYPES](method: TypedMethodBuilder[TYPE, RETURN, PARAMS])(params: IRTYPES)(implicit
      S: Supply[PARAMS, IRTYPES]): $IR[RETURN] =
      invoke(method.asPublic)(params)

    /** Generate an instance method invocation of the given method on this object with the given parameter IR. */
    def invoke[RETURN, PARAMS, IRTYPES](method: PublicTypedMethod[TYPE, RETURN, PARAMS])(params: IRTYPES)(implicit
      S: Supply[PARAMS, IRTYPES]): $IR[RETURN] =
      method.invoke(ir, params)

    /** Generate a less-than comparison if [[TYPE]] is orderable */
    def <(other: $IR[TYPE])(implicit @unused O: Ordering[TYPE]): $IR[Boolean] =
      lessThan(ir, other).typed

    /** Generate a less-than-or-equals comparison if [[TYPE]] is orderable */
    def <=(other: $IR[TYPE])(implicit @unused O: Ordering[TYPE]): $IR[Boolean] =
      lessThanOrEqual(ir, other).typed

    /** Generate a greater-than comparison if [[TYPE]] is orderable */
    def >(other: $IR[TYPE])(implicit @unused O: Ordering[TYPE]): $IR[Boolean] =
      greaterThan(ir, other).typed

    /** Generate a greater-than-or-equals comparison if [[TYPE]] is orderable */
    def >=(other: $IR[TYPE])(implicit @unused O: Ordering[TYPE]): $IR[Boolean] =
      greaterThanOrEqual(ir, other).typed

    /** Generate an [[Add] operation if [[TYPE]] is numeric */
    def +(other: $IR[TYPE])(implicit @unused N: Numeric[TYPE]): $IR[TYPE] =
      Add(ir, other).typed

    /**
     * Generate an [[Add] operation if [[TYPE]] is numeric.
     * Useful when the compiler gets the + operator confused with the string concatenation operator
     */
    infix def add(other: $IR[TYPE])(implicit @unused N: Numeric[TYPE]): $IR[TYPE] =
      Add(ir, other).typed

    /** Generate a [[Subtract]] operation if [[TYPE]] is numeric */
    def -(other: $IR[TYPE])(implicit @unused N: Numeric[TYPE]): $IR[TYPE] =
      Subtract(ir, other).typed

    /** Generate a [[Multiply]] operation if [[TYPE]] is numeric */
    def *(other: $IR[TYPE])(implicit @unused N: Numeric[TYPE]): $IR[TYPE] =
      Multiply(ir, other).typed

    /** Generate a [[BooleanAnd]] operation if [[TYPE]] is boolean */
    def &&(other: $IR[Boolean])(implicit @unused EQ: TYPE =:= Boolean): $IR[Boolean] =
      and(ir, other).typed

    /** Generate a [[BooleanOr]] operation if [[TYPE]] is boolean */
    def ||(other: $IR[Boolean])(implicit @unused EQ: TYPE =:= Boolean): $IR[Boolean] =
      or(ir, other).typed
  }

  /**
   * Enables more natural syntax on typed arrays.
   * Foreach and Map can't be implemented without a [[VariableNamer]] so those are left as part of [[TypedIR]].
   *
   * NB this is a separate class to [[LoadableTypeWrapperExt]] because the inner generic parameter of the Array can't be
   * inferred from an implicit constraint.
   */
  implicit class LoadableArrayTypeWrapperExt[TYPE, INNER](wrapper: TypedWrapper[Array[TYPE], INNER])(implicit
    loadable: LoadAsIR[INNER, Array[TYPE]]) {

    private def arr: $IR[Array[TYPE]] = loadable.load(wrapper)

    def length: $IR[Int] =
      $arrayLength(arr)

    def apply(index: $IR[Int]): $IR[TYPE] =
      $arrayLoad(arr, index)

    def update(index: $IR[Int], value: $IR[TYPE]): $IR[Unit] =
      $arraySet(arr, index, value)
  }

  implicit class TypedWrapperConversion[INNER](inner: INNER) {

    /**
     * Convenience method for applying the strong typing wrapper. Usually the generic [[TYPE]] parameter can be omitted
     * because it will be inferred. NB the programmer must ensure that the static type matches the runtime type!
     */
    def typed[TYPE]: TypedWrapper[TYPE, INNER] =
      new TypedWrapper(inner)
  }

  implicit class TypedFieldExtensions[TYPE](field: $Field[TYPE]) {

    /** Assign the value to the field */
    def :=(value: $IR[TYPE]): $IR[Unit] =
      $setField(field, value)
  }

  implicit class TypedVariableExtensions[TYPE](variable: $Variable[TYPE]) {

    /** Assign the value to the variable */
    def :=(value: $IR[TYPE]): $IR[Unit] =
      $assign(variable, value)

    /** Declare the variable and assign its initial value */
    def declareAndAssign: $IR[Unit] =
      IntermediateRepresentation.declareAndAssign(variable.typ, variable.name, variable.value).typed
  }

  // Type aliases for common wrapped types, using the $ convention

  /** An IntermediateRepresentation assignable to the given type */
  type $IR[+TYPE] = TypedWrapper[TYPE, IntermediateRepresentation]

  /** A TypeReference representing the given type */
  type $Ref[+TYPE] = TypedWrapper[TYPE, TypeReference]

  /** A field of the given type */
  type $Field[+TYPE] = TypedWrapper[TYPE, Field]

  /** A local variable of the given type */
  type $Variable[+TYPE] = TypedWrapper[TYPE, LocalVariable]
}

/**
 * A statically-typed representation of a Method, which can construct typechecked invocations.
 */
class TypedMethodBuilder[OWNER, RETURN, PARAMS](
  ownerType: $Ref[OWNER],
  returnType: $Ref[RETURN],
  name: String,
  paramTypes: TypeList[PARAMS]
) {

  /** Specify the method owner class */
  def on[O: Manifest] =
    new TypedMethodBuilder($typeRef[O], returnType, name, paramTypes)

  /** Specify the method return type */
  def returning[R: Manifest] =
    new TypedMethodBuilder(ownerType, $typeRef[R], name, paramTypes)

  /** Specify the method name */
  def named(newName: String) =
    new TypedMethodBuilder(ownerType, returnType, newName, paramTypes)

  /**
   * Specify the method parameter types: for no parameters, use the unit type; for one parameter, use that type; for
   * multiple parameters, use a tuple of those types
   */
  def withParams[P: TypeList.Make] =
    new TypedMethodBuilder(ownerType, returnType, name, TypeList[P])

  /** Generate a public instance method that can generate an invocation on an instance */
  def asPublic = new PublicTypedMethod(ownerType, returnType, name, paramTypes)

  /** Generate a private/local method that can be invoked without an instance */
  def asPrivate = new PrivateTypedMethod(returnType, name, paramTypes)

  /** Generate a static method that can be invoked without an instance */
  def asStatic = new StaticTypedMethod(ownerType, returnType, name, paramTypes)

  /** Generate a constructor that returns an instance of [[OWNER]]; [[RETURN]] and [[name]] will be ignored */
  def asConstructor = new TypedConstructor(ownerType, paramTypes)
}

object TypedMethod extends TypedMethodBuilder($typeRef[Unit], $typeRef[Unit], "", TypeList.Make.makeEmpty()) {

  def apply[OWNER: Manifest, RETURN: Manifest, PARAMS: TypeList.Make](name: String)
    : TypedMethodBuilder[OWNER, RETURN, PARAMS] =
    new TypedMethodBuilder($typeRef[OWNER], $typeRef[RETURN], name, TypeList[PARAMS])
}

class PublicTypedMethod[OWNER, RETURN, PARAMS](
  ownerType: $Ref[OWNER],
  returnType: $Ref[RETURN],
  name: String,
  paramTypes: TypeList[PARAMS]
) {

  /** Generate an invocation of this method on an instance with the given parameters */
  def invoke[IR](owner: $IR[OWNER], params: IR)(implicit S: TypeList.Supply[PARAMS, IR]): $IR[RETURN] = {
    if (returnType.inner == TypeReference.VOID)
      InvokeSideEffect(owner, Method(ownerType, returnType, name, paramTypes.types), S.apply(params)).typed
    else
      Invoke(owner, Method(ownerType, returnType, name, paramTypes.types), S.apply(params)).typed
  }
}

class PrivateTypedMethod[RETURN, PARAMS](
  returnType: $Ref[RETURN],
  name: String,
  paramTypes: TypeList[PARAMS]
) {

  /** Generate an invocation of this private method with the given parameters */
  def invoke[IR](params: IR)(implicit S: TypeList.Supply[PARAMS, IR]): $IR[RETURN] = {
    if (returnType.inner == TypeReference.VOID)
      InvokeLocalSideEffect(PrivateMethod(returnType, name, paramTypes.types), S.apply(params)).typed
    else
      InvokeLocal(PrivateMethod(returnType, name, paramTypes.types), S.apply(params)).typed
  }

  /** Generate an invocation of this private method with the given parameters */
  def apply[IR](params: IR)(implicit S: TypeList.Supply[PARAMS, IR]): $IR[RETURN] = {
    invoke(params)
  }
}

class StaticTypedMethod[OWNER, RETURN, PARAMS](
  ownerType: $Ref[OWNER],
  returnType: $Ref[RETURN],
  name: String,
  paramTypes: TypeList[PARAMS]
) {

  /** Generate an invocation of this static method with the given parameters */
  def invoke[IR](params: IR)(implicit S: TypeList.Supply[PARAMS, IR]): $IR[RETURN] = {
    if (returnType.inner == TypeReference.VOID)
      InvokeStaticSideEffect(Method(ownerType, returnType, name, paramTypes.types), S.apply(params)).typed
    else
      InvokeStatic(Method(ownerType, returnType, name, paramTypes.types), S.apply(params)).typed
  }

  /** Generate an invocation of this static method with the given parameters */
  def apply[IR](params: IR)(implicit S: TypeList.Supply[PARAMS, IR]): $IR[RETURN] = {
    invoke(params)
  }

}

class TypedConstructor[OWNER, PARAMS](ownerType: $Ref[OWNER], paramTypes: TypeList[PARAMS]) {

  /** Generate an invocation of this constructor with the given parameters, returning an [[OWNER]] instance */
  def invoke[IR](params: IR)(implicit S: TypeList.Supply[PARAMS, IR]): $IR[OWNER] =
    NewInstance(Constructor(ownerType, paramTypes.types), S.apply(params)).typed

  /** Generate an invocation of this constructor with the given parameters, returning an [[OWNER]] instance */
  def apply[IR](params: IR)(implicit S: TypeList.Supply[PARAMS, IR]): $IR[OWNER] =
    invoke(params)
}

/**
 * This class represents a list of types both statically (in [[TYPES]]) and dynamically (in [[types]]).
 * It is important that the two type lists match, for example a `TypeList[(Int, Boolean)]` must have a [[types]] value
 * of `Seq(TypeReference.INT, TypeReference.BOOLEAN)`.
 *
 * The [[Make]] typeclass provides automatic construction for this class, given a matching [[TYPES]] value; and the
 * [[Supply]] typeclass provides a constraint that ensures matching IR types are provided.
 */
class TypeList[TYPES] private (val types: Seq[TypeReference])

object TypeList {
  def apply[TYPES](implicit make: Make[TYPES]): TypeList[TYPES] = make()

  /** Typeclass that binds a static [[TYPES]] list to a TypeList with the right dynamic types */
  sealed trait Make[TYPES] {
    def apply(): TypeList[TYPES]
  }

  object Make {

    private def create[A](types: TypeReference*): Make[A] =
      new Make[A] {
        def apply(): TypeList[A] = new TypeList[A](types)
      }

    // the implementations below can be made much prettier in scala 3 where tuples are already a heterogenous list
    implicit val makeEmpty: Make[Unit] = create()
    implicit def make1[A: Manifest]: Make[A] = create(typeRefOf[A])
    implicit def make2[A: Manifest, B: Manifest]: Make[(A, B)] = create(typeRefOf[A], typeRefOf[B])

    implicit def make3[A: Manifest, B: Manifest, C: Manifest]: Make[(A, B, C)] =
      create(typeRefOf[A], typeRefOf[B], typeRefOf[C])

    implicit def make4[A: Manifest, B: Manifest, C: Manifest, D: Manifest]: Make[(A, B, C, D)] =
      create(typeRefOf[A], typeRefOf[B], typeRefOf[C], typeRefOf[D])

    implicit def make5[A: Manifest, B: Manifest, C: Manifest, D: Manifest, E: Manifest]: Make[(A, B, C, D, E)] =
      create(typeRefOf[A], typeRefOf[B], typeRefOf[C], typeRefOf[D], typeRefOf[E])

    implicit def make6[A: Manifest, B: Manifest, C: Manifest, D: Manifest, E: Manifest, F: Manifest]
      : Make[(A, B, C, D, E, F)] =
      create(typeRefOf[A], typeRefOf[B], typeRefOf[C], typeRefOf[D], typeRefOf[E], typeRefOf[F])

    implicit def make7[A: Manifest, B: Manifest, C: Manifest, D: Manifest, E: Manifest, F: Manifest, G: Manifest]
      : Make[(A, B, C, D, E, F, G)] =
      create(typeRefOf[A], typeRefOf[B], typeRefOf[C], typeRefOf[D], typeRefOf[E], typeRefOf[F], typeRefOf[G])

    implicit def make8[
      A: Manifest,
      B: Manifest,
      C: Manifest,
      D: Manifest,
      E: Manifest,
      F: Manifest,
      G: Manifest,
      H: Manifest
    ]: Make[(A, B, C, D, E, F, G, H)] =
      create(
        typeRefOf[A],
        typeRefOf[B],
        typeRefOf[C],
        typeRefOf[D],
        typeRefOf[E],
        typeRefOf[F],
        typeRefOf[G],
        typeRefOf[H]
      )

    implicit def make9[
      A: Manifest,
      B: Manifest,
      C: Manifest,
      D: Manifest,
      E: Manifest,
      F: Manifest,
      G: Manifest,
      H: Manifest,
      I: Manifest
    ]: Make[(A, B, C, D, E, F, G, H, I)] =
      create(
        typeRefOf[A],
        typeRefOf[B],
        typeRefOf[C],
        typeRefOf[D],
        typeRefOf[E],
        typeRefOf[F],
        typeRefOf[G],
        typeRefOf[H],
        typeRefOf[I]
      )

    implicit def make10[
      A: Manifest,
      B: Manifest,
      C: Manifest,
      D: Manifest,
      E: Manifest,
      F: Manifest,
      G: Manifest,
      H: Manifest,
      I: Manifest,
      J: Manifest
    ]: Make[(A, B, C, D, E, F, G, H, I, J)] =
      create(
        typeRefOf[A],
        typeRefOf[B],
        typeRefOf[C],
        typeRefOf[D],
        typeRefOf[E],
        typeRefOf[F],
        typeRefOf[G],
        typeRefOf[H],
        typeRefOf[I],
        typeRefOf[J]
      )

    implicit def make11[
      A: Manifest,
      B: Manifest,
      C: Manifest,
      D: Manifest,
      E: Manifest,
      F: Manifest,
      G: Manifest,
      H: Manifest,
      I: Manifest,
      J: Manifest,
      K: Manifest
    ]: Make[(A, B, C, D, E, F, G, H, I, J, K)] =
      create(
        typeRefOf[A],
        typeRefOf[B],
        typeRefOf[C],
        typeRefOf[D],
        typeRefOf[E],
        typeRefOf[F],
        typeRefOf[G],
        typeRefOf[H],
        typeRefOf[I],
        typeRefOf[J],
        typeRefOf[K]
      )

    implicit def make12[
      A: Manifest,
      B: Manifest,
      C: Manifest,
      D: Manifest,
      E: Manifest,
      F: Manifest,
      G: Manifest,
      H: Manifest,
      I: Manifest,
      J: Manifest,
      K: Manifest,
      L: Manifest
    ]: Make[(A, B, C, D, E, F, G, H, I, J, K, L)] =
      create(
        typeRefOf[A],
        typeRefOf[B],
        typeRefOf[C],
        typeRefOf[D],
        typeRefOf[E],
        typeRefOf[F],
        typeRefOf[G],
        typeRefOf[H],
        typeRefOf[I],
        typeRefOf[J],
        typeRefOf[K],
        typeRefOf[L]
      )

  }

  /** Typeclass providing a constraint on provided IR values to make sure that they correspond to the static types in
   * [[TYPES]] */
  @implicitNotFound(
    "Typecheck failed for invocation parameters in generated code. Make sure that all parameters in ${IRTYPES} are of type $IR[...] and that they match the declared types ${TYPES} for the method"
  )
  sealed trait Supply[TYPES, IRTYPES] {
    def apply(ir: IRTYPES): Seq[IntermediateRepresentation]
  }

  object Supply {

    private def create[TYPES, IRTYPES](f: IRTYPES => Seq[IntermediateRepresentation]): Supply[TYPES, IRTYPES] =
      new Supply[TYPES, IRTYPES] {
        def apply(ir: IRTYPES): Seq[IntermediateRepresentation] = f(ir)
      }

    implicit val supply0: Supply[Unit, Unit] = create(_ => Seq.empty)
    implicit def supply1[A]: Supply[A, $IR[A]] = create(ir => Seq(ir))
    implicit def supply2[A, B]: Supply[(A, B), ($IR[A], $IR[B])] = create(ir => Seq(ir._1, ir._2))
    implicit def supply3[A, B, C]: Supply[(A, B, C), ($IR[A], $IR[B], $IR[C])] = create(ir => Seq(ir._1, ir._2, ir._3))

    implicit def supply4[A, B, C, D]: Supply[(A, B, C, D), ($IR[A], $IR[B], $IR[C], $IR[D])] =
      create(ir => Seq(ir._1, ir._2, ir._3, ir._4))

    implicit def supply5[A, B, C, D, E]: Supply[(A, B, C, D, E), ($IR[A], $IR[B], $IR[C], $IR[D], $IR[E])] =
      create(ir => Seq(ir._1, ir._2, ir._3, ir._4, ir._5))

    implicit def supply6[A, B, C, D, E, F]
      : Supply[(A, B, C, D, E, F), ($IR[A], $IR[B], $IR[C], $IR[D], $IR[E], $IR[F])] =
      create(ir => Seq(ir._1, ir._2, ir._3, ir._4, ir._5, ir._6))

    implicit def supply7[A, B, C, D, E, F, G]
      : Supply[(A, B, C, D, E, F, G), ($IR[A], $IR[B], $IR[C], $IR[D], $IR[E], $IR[F], $IR[G])] =
      create(ir => Seq(ir._1, ir._2, ir._3, ir._4, ir._5, ir._6, ir._7))

    implicit def supply8[A, B, C, D, E, F, G, H]
      : Supply[(A, B, C, D, E, F, G, H), ($IR[A], $IR[B], $IR[C], $IR[D], $IR[E], $IR[F], $IR[G], $IR[H])] =
      create(ir => Seq(ir._1, ir._2, ir._3, ir._4, ir._5, ir._6, ir._7, ir._8))

    implicit def supply9[A, B, C, D, E, F, G, H, I]
      : Supply[(A, B, C, D, E, F, G, H, I), ($IR[A], $IR[B], $IR[C], $IR[D], $IR[E], $IR[F], $IR[G], $IR[H], $IR[I])] =
      create(ir => Seq(ir._1, ir._2, ir._3, ir._4, ir._5, ir._6, ir._7, ir._8, ir._9))

    implicit def supply10[A, B, C, D, E, F, G, H, I, J]: Supply[
      (A, B, C, D, E, F, G, H, I, J),
      ($IR[A], $IR[B], $IR[C], $IR[D], $IR[E], $IR[F], $IR[G], $IR[H], $IR[I], $IR[J])
    ] = create(ir => Seq(ir._1, ir._2, ir._3, ir._4, ir._5, ir._6, ir._7, ir._8, ir._9, ir._10))

    implicit def supply11[A, B, C, D, E, F, G, H, I, J, K]: Supply[
      (A, B, C, D, E, F, G, H, I, J, K),
      ($IR[A], $IR[B], $IR[C], $IR[D], $IR[E], $IR[F], $IR[G], $IR[H], $IR[I], $IR[J], $IR[K])
    ] = create(ir => Seq(ir._1, ir._2, ir._3, ir._4, ir._5, ir._6, ir._7, ir._8, ir._9, ir._10, ir._11))

    implicit def supply12[A, B, C, D, E, F, G, H, I, J, K, L]: Supply[
      (A, B, C, D, E, F, G, H, I, J, K, L),
      ($IR[A], $IR[B], $IR[C], $IR[D], $IR[E], $IR[F], $IR[G], $IR[H], $IR[I], $IR[J], $IR[K], $IR[L])
    ] = create(ir => Seq(ir._1, ir._2, ir._3, ir._4, ir._5, ir._6, ir._7, ir._8, ir._9, ir._10, ir._11, ir._12))
  }
}
