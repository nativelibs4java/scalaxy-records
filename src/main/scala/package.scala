package scalaxy

import scala.language.higherKinds
import scala.language.experimental.macros

import scala.reflect.macros.blackbox.Context
import scala.reflect.runtime.universe.TypeTag
import scala.runtime.IntRef

import scalaxy.reified._

package records {

  trait Record[T[_[_]] <: Record[T]] {
    type Single = T[Identity]

    type Array = T[scala.Array] // with ArrayLike
    type ArrayFactory = Int => Array

    type IO[R] = T[FieldIO[R]#Field]

    trait FieldIO[R] {
      type Field[V] = FieldIOLike[R, V]
    }

    // TODO(ochafik): How do cursors perform compared to reified getters?
    type Cursor = CursorLike with T[Accessors]
    type CursorFactory = (Array, Int) => Cursor

    // Cursor that uses a source array and lazily
    type CachedCursor = CursorLike with T[CachedAccessor]
    case class CachedCursorFactoryArgs[Source](
        sourceArray: scala.Array[Source],
        sourceLength: Int,
        storage: Array,
        io: IO[Source])
    type CachedCursorFactory[Source] = CachedCursorFactoryArgs[Source] => CachedCursor

    type RecordFieldGetter[V] = Reified[(Array, Int) => V]
    type Getters = T[RecordFieldGetter]
  }


  trait FieldIOLike[R, V] {
    def read(row: R): V
    def write(row: R, value: V): Unit
  }

//  trait ArrayLike {
//    def length: Int
//  }

  trait CursorLike { //extends ArrayLike {
    val length: Int
    protected var row: Int = -1
    protected def setRow(value: Int): Unit = row = value
    def reset = setRow(-1)
//    def at(row: Int) {
//      if (row < -1 || row >= length) {
//        new IndexOutOfBoundsException(s"requested row = $row, length = $length, current row = ${this.row}")
//      }
//      this.row = row
//    }
    @inline final def next(): Boolean = {
      val nextRow = row + 1
      if (nextRow < length) {
        setRow(nextRow)
        true
      } else {
        false
      }
    }
  }

  trait Accessors[@specialized V] {
    def apply(): V
    def update(value: V): Unit
  }

  abstract class CachedAccessor[V](fieldBitMask: Int, rowAccessedFieldsBits: IntRef) extends Accessors[V] {
    assert(
      fieldBitMask != 0 && Integer.highestOneBit(fieldBitMask) == Integer.lowestOneBit(fieldBitMask),
      s"Invalid fieldBitMask: $fieldBitMask")

    def isFirstAccess: Boolean = {
      val bits = rowAccessedFieldsBits.elem
      if ((bits & fieldBitMask) != 0) {
        false
      } else {
        // TODO(ochafik): compare and set + AtomicInteger, loop until gets a chance?
        rowAccessedFieldsBits.elem = bits | fieldBitMask
        true
      }
    }
  }


}
package object records {

  type Identity[T] = T

  private[this] def debugResult[V](v: V): V = {
    println("result = " + v)
    v
  }

  /**
   * Given the following record:
   *
   * ```
   *   trait User[C[_]] extends Record[User] {
   *     val name: C[String]
   *     val birthDate: C[Date]
   *     val id: C[Long]
   *   }
   * ```
   *
   * This method would return:
   * `(typeOf[User], C, Map(name -> typeOf[String], birthDate -> typeOf[Date], id -> typeOf[Long]))`
   */
  private[this]
  def getRecordInfo
      [R[_[_]] <: Record[R]]
      (c: Context)
      : (c.Type, c.Type, Map[c.universe.TermName, c.Type]) =
  {
    import c.universe._

    try {
      // Would normally bring a c.WeakTypeTag, but it seems to not work with higher-kinded types here, so getting the type straight from the macro call.
      val typeClassTpe = c.macroApplication match {
        case q"${_}[$typeClassTpt]" => typeClassTpt.tpe
        case q"${_}[$typeClassTpt](..${_})" => typeClassTpt.tpe
      }

      // Get the unique type parameter (`C[_]` in the example above).
      val List(typeParam) = typeClassTpe.typeConstructor.typeParams

//      println(s"term.typeSignature = ${term.typeSignature}")
//      println(s"typeClassTpe = $typeClassTpe")
//      println(s"typeParam = $typeParam")
//      println(s"c.macroApplication.tpe = ${c.macroApplication.tpe}")
      def toSeq(tpe: Type) = {
        val b = Seq.newBuilder[Type]
        tpe.foreach(b += _)
        b.result()
      }

      val typeArg: Type =
        toSeq(c.macroApplication.tpe).toIterator.map({ tpe =>
          tpe.baseType(typeClassTpe.typeSymbol)
        }).
        collectFirst({
          case TypeRef(_, _, List(typeArg)) =>
            typeArg
        })
        .getOrElse({
          c.error(c.enclosingPosition, s"Failed to find a value for $typeParam (in $typeClassTpe) within ${c.macroApplication.tpe}")
          null
        })

      val actualTypeClassTpe = internal.typeRef(typeClassTpe, typeClassTpe.typeSymbol, List(typeArg))
      def replaceTypeParam(tpe: Type) =
        tpe.substituteSymbols(List(typeParam), List(typeArg.typeSymbol))

      val abstractTerms =
        actualTypeClassTpe.members.filter(_.isTerm).map(_.asTerm).filter(_.isAbstract).toSeq
          .map({ term =>

            val fieldTpe = replaceTypeParam(if (term.isMethod) term.asMethod.returnType else term.typeSignature)

            val baseType = fieldTpe.baseType(typeArg.typeSymbol)
            if (baseType == NoType) {
              c.error(c.enclosingPosition, s"Type $fieldTpe of $actualTypeClassTpe.${term.name} (sym = $term, tpe = ${term.typeSignature}) does not conform to ${typeArg.typeSymbol}")
            }
          // Get the type argument given to `C[_]`. For instance gets `String` out of `C[String]`.
            val TypeRef(_, _, List(tpe)) = baseType
            term.name -> tpe
          }).toMap

      val res = (
        actualTypeClassTpe,
        typeArg,//.typeSymbol,
        abstractTerms
      )

//      println(s"""
//        typeClassTpe = $typeClassTpe
//        typeArg = $typeArg
//        actualTypeClassTpe = $actualTypeClassTpe
//        abstractTerms =
//          ${abstractTerms.toSeq.mkString("\n          ")}
//      """)

      res
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        c.error(c.enclosingPosition, ex.toString)
        null
    }
  }

  def recordArrayFactory[R[_[_]] <: Record[R]]: Record[R]#ArrayFactory =
    macro recordFactoryImpl[R]

  def recordFactoryImpl
      [R[_[_]] <: Record[R]]
      (c: Context)
      : c.Expr[Record[R]#ArrayFactory] =
  {
    import c.universe._

    val (typeClassTpe, typeParam, abstractTerms) = getRecordInfo[R](c)

    val sizeName = TermName(c.freshName("size"))
    val decls = abstractTerms.map { case (name, tpe) =>
      q"override val $name = new scala.Array[$tpe]($sizeName)"
    }
    debugResult(c.Expr[Record[R]#ArrayFactory](q"""
      ($sizeName: Int) => new ${typeClassTpe.typeSymbol}[scala.Array]
          //with scalaxy.records.ArrayLike
          {
        ..$decls
      }
    """))
    //override val length = $sizeName
  }

  def recordGetters[R[_[_]] <: Record[R]]: Record[R]#Getters =
    macro recordGettersImpl[R]

  def recordGettersImpl
      [R[_[_]] <: Record[R]]
      (c: Context)
      : c.Expr[Record[R]#Getters] =
  {
    import c.universe._

    val (typeClassTpe, typeParam, abstractTerms) = getRecordInfo[R](c)

    val arrayTpe = tq"${typeClassTpe.typeSymbol}[scala.Array]"
    val decls = abstractTerms.map {
      case (name, tpe) =>
        q"""
          override val $name = scalaxy.reified.reified[($arrayTpe, Int) => $tpe] {
            (record: $arrayTpe, row: Int) => record.$name(row)
          }
        """
    }
    debugResult(c.Expr[Record[R]#Getters](q"""
      new scalaxy.records.Record[${typeClassTpe.typeSymbol}]#Getters { ..$decls }
    """))
  }

  def recordCursorFactory[R[_[_]] <: Record[R]]: Record[R]#CursorFactory =
    macro recordCursorFactoryImpl[R]

  def recordCursorFactoryImpl
      [R[_[_]] <: Record[R]]
      (c: Context)
      : c.Expr[Record[R]#CursorFactory] =
  {
    import c.universe._

    val (typeClassTpe, typeParam, abstractTerms) = getRecordInfo[R](c)

    val arrayTpe = tq"${typeClassTpe.typeSymbol}[scala.Array]"
    val arrayName = TermName(c.freshName("array"))
    val lengthName = TermName(c.freshName("length"))

    debugResult(c.Expr[Record[R]#CursorFactory](q"""
      ($arrayName: $arrayTpe, $lengthName: Int) =>
        ${recordCursorImpl[R](c)(c.Expr[Record[R]#Array](q"$arrayName"), c.Expr[Int](q"$lengthName"))}
    """))
  }


  def recordCursor[R[_[_]] <: Record[R]](array: Record[R]#Array, length: Int): Record[R]#Cursor =
    macro recordCursorImpl[R]

  def recordCursorImpl
      [R[_[_]] <: Record[R]]
      (c: Context)
      (array: c.Expr[Record[R]#Array], length: c.Expr[Int])
      : c.Expr[Record[R]#Cursor] =
  {
    import c.universe._

    val (typeClassTpe, typeParam, abstractTerms) = getRecordInfo[R](c)

    val arrayName = TermName(c.freshName("array"))

    val decls = abstractTerms.map {
      case (name, tpe) =>
        q"""
          override val $name = new scalaxy.records.Accessors[$tpe] {
            @inline def apply() = $arrayName.$name(row)
            @inline def update(value: $tpe) = $arrayName.$name(row) = value
          }
        """
    }
    debugResult(c.Expr[Record[R]#Cursor](q"""
        new ${typeClassTpe.typeSymbol}[scalaxy.records.Accessors] with scalaxy.records.CursorLike {
          private[this] val $arrayName = $array
          override val length = $length
          ..$decls
        }
    """))
  }

//  def cachedCursorFactory[Source, R[_[_]] <: Record[R]]: Record[R]#CachedCursorFactory[Source] =
//    macro cachedCursorFactoryImpl[R]
//
//  def cachedCursorFactoryImpl
//      [Source, R[_[_]] <: Record[R]]
//      (c: Context)
//      : c.Expr[Record[R]#CachedCursorFactory[Source]] =
//  {
//    import c.universe._
//
//    val (typeClassTpe, typeParam, abstractTerms) = getRecordInfo(c)
//
//    val argsTpe = tq"$scalaxy.records.CursorLike"
//    val argsName = TermName(c.freshName("args"))
//
//    val decls = abstractTerms.map {
//      case (name, tpe) =>
//        q"""
//          override val $name = new scalaxy.records.Accessors[$tpe] {
//            def apply() = $arrayName.$name(row)
//            def update(value: $tpe) = $arrayName.$name(row) = value
//          }
//        """
//    }
//    c.Expr[Record[R]#CachedCursorFactory[Source]](q"""
//      ($argsName: $argsTpe) =>
//        new ${typeClassTpe.typeSymbol}[scalaxy.records.CachedAccessors]
//            with $cursorLikeTpe {
//          override val length = $arrayName.${abstractTerms.keys.iterator.next}.length
//          ..$decls
//        }
//    """)
//  }

  /*
  class CachedUserCursor[R](records: scala.Array[R], array: Record[User]#Array, io: Record[User]#IO[R]) extends User[Accessors] with CursorLike {//Record[User]#Cursor {
    val length = array.name.length
    private[this] val accessedFieldsArray = new scala.Array[Int](length)
    private[this] val currentAccessedFields = new IntRef(0)
    override def setRow(value: Int) {
      super.setRow(value)
      currentAccessedFields.elem = if (value < 0) 0 else accessedFieldsArray(value)
    }

    val name = new CachedAccessor[String](1 << 0, currentAccessedFields) {
      def apply() = {
        if (isFirstAccess) this() = io.name.read(records(row))
        array.name(row)
      }
      def update(value: String) = array.name(row) = value
    }
    val birthDate = new CachedAccessor[Date](1 << 1, currentAccessedFields) {
      def apply() = {
        if (isFirstAccess) this() = io.birthDate.read(records(row))
        array.birthDate(row)
      }
      def update(value: Date) = array.birthDate(row) = value
    }
    val kudosCount = new CachedAccessor[Int](1 << 2, currentAccessedFields) {
      def apply() = {
        if (isFirstAccess) this() = io.kudosCount.read(records(row))
        array.kudosCount(row)
      }
      def update(value: Int) = array.kudosCount(row) = value
    }
  }
  */
}
