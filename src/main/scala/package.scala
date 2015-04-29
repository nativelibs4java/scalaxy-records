package scalaxy

import scala.language.higherKinds
import scala.language.experimental.macros

import scala.reflect.macros.blackbox.Context
import scala.reflect.runtime.universe.TypeTag

import scalaxy.reified._

package object records {

  trait Record[T[_[_]]] {
    type Array = T[scala.Array]
    type Factory = Int => Array
    type RecordFieldGetter[V] = Reified[(Array, Int) => V]
    type Getters = T[RecordFieldGetter]
  }

  def recordFactory[R[_[_]] <: Record[R]]: Record[R]#Factory =
    macro recordFactoryImpl[R]

  private[this] def getRecordInfo(c: Context): (c.Type, c.Symbol, Map[c.universe.TermName, c.Type]) = {

    import c.universe._

    val TypeApply(_, List(typeClassTpt)) = c.macroApplication
    val typeClassTpe = typeClassTpt.tpe
    val List(typeParam) = typeClassTpe.typeConstructor.typeParams
//    println(s"Typeclass: ${typeClassTpe}; arg = ${typeParam}")

    val abstractTerms =
      typeClassTpe.members.filter(_.isTerm).map(_.asTerm).filter(_.isAbstract).toSeq
        .map({ term =>
          val TypeRef(_, _, List(tpe)) = term.typeSignature.baseType(typeParam)
          term.name -> tpe
        }).toMap

//    println(s"""
//      Typeclass: $typeClassTpe
//      arg = $typeParam
//      members:\n\t${abstractTerms.mkString("\n\t")}
//    """)

    (
      typeClassTpe,
      typeParam,
      abstractTerms
    )
  }
  def recordFactoryImpl
      [R[_[_]] <: Record[R]]
      (c: Context)
      : c.Expr[Record[R]#Factory] =
  {
    import c.universe._

    val (typeClassTpe, typeParam, abstractTerms) = getRecordInfo(c)

    val sizeName = TermName(c.freshName("size"))
    val decls = abstractTerms.map { case (name, tpe) =>
      q"override val $name = new scala.Array[$tpe]($sizeName)"
    }
    c.Expr[Record[R]#Factory](q"""
      ($sizeName: Int) =>
        new ${typeClassTpe.typeSymbol}[scala.Array] { ..$decls }
    """)
  }

  def recordGetters[R[_[_]] <: Record[R]]: Record[R]#Getters =
    macro recordGettersImpl[R]

  def recordGettersImpl
      [R[_[_]] <: Record[R]]
      (c: Context)
      : c.Expr[Record[R]#Getters] =
  {
    import c.universe._

    val (typeClassTpe, typeParam, abstractTerms) = getRecordInfo(c)

    val arrayTpe = tq"${typeClassTpe.typeSymbol}[scala.Array]"

    val decls = abstractTerms.map { case (name, tpe) =>
      q"""
        override val $name =
          scalaxy.reified.reified[($arrayTpe, Int) => $tpe] {
            (record: $arrayTpe, row: Int) => record.$name(row)
          }
      """
    }
    c.Expr[Record[R]#Getters](q"""
      new scalaxy.records.Record[$typeClassTpe]#Getters { ..$decls }
    """)
  }
}
