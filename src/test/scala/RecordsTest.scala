package scalaxy.records
package test

import java.util.Date
import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.{ FlatSpecLike, Matchers }

import scala.language.higherKinds
import scala.runtime.IntRef

import scalaxy.reified._


// val users = new Array[User](100000)
// ...
// for (user <- users) {
//   print(s"${user.name}: ${user.kudosCount}")
// }

// val users = new UserArray(1000000)
// ...
// val it = new UserCursor(users)
// while (it.next()) {
//   print(s"${user.name()}: ${user.kudosCount()}")
// }

trait User[C[_]] extends Record[User] {
  val name: C[String]
  val birthDate: C[Date]
  val kudosCount: C[Int]
//  def address: Address[C]
}
class UserGetters extends Record[User]#Getters {
  override val kudosCount = reified[Function2[User[scala.Array], Int, Int]](((record: User[scala.Array], row: Int) => record.kudosCount(row)));
  override val birthDate = reified[Function2[User[scala.Array], Int, java.util.Date]](((record: User[scala.Array], row: Int) => record.birthDate(row)));
  override val name = reified[Function2[User[scala.Array], Int, String]](((record: User[scala.Array], row: Int) => record.name(row)))
};

trait Address[C[_]] extends Record[Address] {
  val zipCode: C[String]
  val city: C[String]
}
class UserArray(size: Int) extends User[scala.Array] {
  override val name = new scala.Array[String](size)
  override val birthDate = new scala.Array[Date](size)
  override val kudosCount = new scala.Array[Int](size)
//  val address = new AddressArray(size)
}
class AddressArray(size: Int) extends Address[scala.Array] {
  override val zipCode = new scala.Array[String](size)
  override val city = new scala.Array[String](size)
}

//class UserPointer(size: Int) extends User[Pointer] {
//  val name = Pointer.allocateArray(classOf[String], size)
//  val birthDate = Pointer.allocateArray(classOf[Date], size)
//  val kudosCount = Pointer.allocateArray(classOf[Int], size)
//}

class UserCursor(val length: Int, array: Record[User]#Array) extends User[Accessors] with CursorLike {//Record[User]#Cursor {
  override val name = new Accessors[String] {
    @inline def apply() = array.name(row)
    @inline def update(value: String) = array.name(row) = value
  }
  override val birthDate = new Accessors[Date] {
    @inline def apply() = array.birthDate(row)
    @inline def update(value: Date) = array.birthDate(row) = value
  }
  override val kudosCount = new Accessors[Int] {
    @inline def apply() = array.kudosCount(row)
    @inline def update(value: Int) = array.kudosCount(row) = value
  }
//  override val address = new Address[Accessors] with CursorLike {
//    override val length = UserCursor.this.length
//    val zipCode = new Accessors[String] {
//      def apply() = array.address.zipCode(row)
//      def update(value: String) = array.address.zipCode(row) = value
//    }
//    val city = new Accessors[String] {
//      def apply() = array.address.city(row)
//      def update(value: String) = array.address.city(row) = value
//    }
//  }
}


//class UserIO extends User[FieldIO] with CursorLike {//Record[User]#Cursor {
//  val name = rowStringReader(0)
//  val birthDate = rowDateReader(1)
//  val kudosCount = rowIntReader(2)
// }
// class UserIO extends User[Accessors] with CursorLike {//Record[User]#Cursor {
//   val name = FieldIOLike[Row, String] {
//     def read(row: Row): String = row.getString(0)
//     def write(row: Row, value: String): Unit = row.setString(0, value)
//   }
//   val birthDate = FieldIOLike[Row, Date] {
//     def read(row: Row): Date = row.getDate(1)
//     def write(row: Row, value: Date): Unit = row.setDate(1, value)
//   }
//   val kudosCount = FieldIOLike[Row, Int] {
//     def read(row: Row): Int = row.getInt(2)
//     def write(row: Row, value: Int): Unit = row.setInt(2, value)
//   }
// }

//A <: Record[_[_]]
//B <: Record[_[_]]
//f: Reified[A => B]

//reified {
//  val it = new Cursor(n, myArray)
//  while (it) {
//    //f(it)
//    it.name() ...
//    // it.birthDate() ...
//    if (outputcursor.isFull) outputCursor.dump();
//  }
//}
//
//for (v <- ...) {
//  v.name()
//}

class CachedUserCursor[R](val length: Int, records: scala.Array[R], array: Record[User]#Array, io: Record[User]#IO[R]) extends User[Accessors] with CursorLike {//Record[User]#Cursor {
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
//  val address = new Address[Accessors] with CursorLike {
//    override val length = CachedUserCursor.this.length
//    val zipCode = new CachedAccessor[String](1 << 0, currentAccessedFields) {
//      def apply() = {
//        if (isFirstAccess) this() = io.address.zipCode.read(records(row))
//        array.address.zipCode(row)
//      }
//      def update(value: String) = array.address.zipCode(row) = value
//    }
//    val city = new CachedAccessor[String](1 << 0, currentAccessedFields) {
//      def apply() = {
//        if (isFirstAccess) this() = io.address.city.read(records(row))
//        array.address.city(row)
//      }
//      def update(value: String) = array.address.city(row) = value
//    }
//  }
}


object Cassandra {

  trait Row

  trait RowFieldReader[V] {
    def readField(row: Row): V
  }

  type Reader[T[_[_]] <: Record[T]] = T[RowFieldReader]

  trait CachedArray[T[_[_]] <: Record[T]] {
    val array: Record[T]#Array
    val reader: Reader[T]
  }
}

class RecordArraysTest extends FlatSpecLike with Matchers {

  behavior of "RecordArrays"

  it should "work" in {

//    def foo[[V] Array[V]] = ???

    val n = 10000000
    val times = 30

    val cursorRunner = new CursorRunner(n)
    val arrayRunner = new ArrayRunner(n)
    for (i <- 0 until times) {
      val r1 = time("sum with cursor") {
        cursorRunner.sum
      }
      val r2 = time("sum with array ") {
        arrayRunner.sum
      }
      if (r1 != r2) sys.error("err!")
      println()
    }
  }

//
//    println("users: " + users)
//    println("name: " + users.name.toSeq)
//    println("birthDate: " + users.birthDate.toSeq)
//    println("kudosCount: " + users.kudosCount.toSeq)
//    for (i <- 0 until n) {
////       val name = userGetters.name(users, i)
////       println(s"i = $i, name = $name")
//    }
}
