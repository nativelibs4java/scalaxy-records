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
//  val name: C[String]
//  val birthDate: C[Date]
  val kudosCount: C[Int]
  val a: C[Double]
  val b: C[Double]
  val c: C[Double]
  val d: C[Double]
  val e: C[Double]
  val f: C[Double]
  val g: C[Double]
  val h: C[Double]
  val i: C[Double]
  val j: C[Double]
  val k: C[Double]
  val l: C[Double]
//  def address: Address[C]
}
class UserGetters extends Record[User]#Getters {
//  override val birthDate = reified[Function2[User[scala.Array], Int, java.util.Date]](((record: User[scala.Array], row: Int) => record.birthDate(row)));
//  override val name = reified[Function2[User[scala.Array], Int, String]](((record: User[scala.Array], row: Int) => record.name(row)))
  override val kudosCount = reified[Function2[User[scala.Array], Int, Int]](((record: User[scala.Array], row: Int) => record.kudosCount(row)));
  override val a = reified[Function2[User[scala.Array], Int, Double]]((record: User[scala.Array], row: Int) => record.a(row))
  override val b = reified[Function2[User[scala.Array], Int, Double]]((record: User[scala.Array], row: Int) => record.b(row))
  override val c = reified[Function2[User[scala.Array], Int, Double]]((record: User[scala.Array], row: Int) => record.c(row))
  override val d = reified[Function2[User[scala.Array], Int, Double]]((record: User[scala.Array], row: Int) => record.d(row))
  override val e = reified[Function2[User[scala.Array], Int, Double]]((record: User[scala.Array], row: Int) => record.e(row))
  override val f = reified[Function2[User[scala.Array], Int, Double]]((record: User[scala.Array], row: Int) => record.f(row))
  override val g = reified[Function2[User[scala.Array], Int, Double]]((record: User[scala.Array], row: Int) => record.g(row))
  override val h = reified[Function2[User[scala.Array], Int, Double]]((record: User[scala.Array], row: Int) => record.h(row))
  override val i = reified[Function2[User[scala.Array], Int, Double]]((record: User[scala.Array], row: Int) => record.i(row))
  override val j = reified[Function2[User[scala.Array], Int, Double]]((record: User[scala.Array], row: Int) => record.j(row))
  override val k = reified[Function2[User[scala.Array], Int, Double]]((record: User[scala.Array], row: Int) => record.k(row))
  override val l = reified[Function2[User[scala.Array], Int, Double]]((record: User[scala.Array], row: Int) => record.l(row))
};

trait Address[C[_]] extends Record[Address] {
  val zipCode: C[String]
  val city: C[String]
}
class UserArray(size: Int) extends User[scala.Array] {
//  override val name = new scala.Array[String](size)
//  override val birthDate = new scala.Array[Date](size)
 override val kudosCount = new scala.Array[Int](size)
  override val a = new scala.Array[Double](size)
  override val b = new scala.Array[Double](size)
  override val c = new scala.Array[Double](size)
  override val d = new scala.Array[Double](size)
  override val e = new scala.Array[Double](size)
  override val f = new scala.Array[Double](size)
  override val g = new scala.Array[Double](size)
  override val h = new scala.Array[Double](size)
  override val i = new scala.Array[Double](size)
  override val j = new scala.Array[Double](size)
  override val k = new scala.Array[Double](size)
  override val l = new scala.Array[Double](size)
//  val address = new AddressArray(size)
}
object buffers {

  import java.nio._
  import scala.reflect.ClassTag
  // import scala.reflect.runtime.universe._

  //  import scala.reflect.runtime.universe
  trait CanBuildDirectBuffer[A, B <: java.nio.Buffer] {
    val componentSize: Int
    def createByteBuffer(size: Int) = ByteBuffer.allocateDirect(componentSize * size)
    def wrapBuffer(b: ByteBuffer): B
    def createTypedBuffer(size: Int) =
      wrapBuffer(createByteBuffer(size))
  }

  implicit object CanBuildDirectIntBuffer extends CanBuildDirectBuffer[Int, IntBuffer] {
    override val componentSize = 4
    override def wrapBuffer(b: ByteBuffer) = b.asIntBuffer()
  
  }

  implicit object CanBuildDirectLongBuffer extends CanBuildDirectBuffer[Long, LongBuffer] {
    override val componentSize = 8
    override def wrapBuffer(b: ByteBuffer) = b.asLongBuffer()
  }

  implicit object CanBuildDirectShortBuffer extends CanBuildDirectBuffer[Short, ShortBuffer] {
    override val componentSize = 2
    override def wrapBuffer(b: ByteBuffer) = b.asShortBuffer()
  }

  implicit object CanBuildDirectCharBuffer extends CanBuildDirectBuffer[Char, CharBuffer] {
    override val componentSize = 2
    override def wrapBuffer(b: ByteBuffer) = b.asCharBuffer()
  }

  implicit object CanBuildDirectByteBuffer extends CanBuildDirectBuffer[Byte, ByteBuffer] {
    override val componentSize = 1
    override def wrapBuffer(b: ByteBuffer) = b
  }

  implicit object CanBuildDirectDoubleBuffer extends CanBuildDirectBuffer[Double, DoubleBuffer] {
    override val componentSize = 8
    override def wrapBuffer(b: ByteBuffer) = b.asDoubleBuffer()
  }

  implicit object CanBuildDirectFloatBuffer extends CanBuildDirectBuffer[Float, FloatBuffer] {
    override val componentSize = 4
    override def wrapBuffer(b: ByteBuffer) = b.asFloatBuffer()
  }

  sealed trait TypedBuffer[A]
  final class BufferWrapper
      [A, B <: java.nio.Buffer]
      (val buffer: ByteBuffer, val typedBuffer: B)
      extends TypedBuffer[A]

  object TypedBuffer {
    class TypedBufferFactory[A] {
      def apply
          [B <: java.nio.Buffer]
          (size: Int)
          (implicit ev: CanBuildDirectBuffer[A, B])
          : BufferWrapper[A, B] = {
        val b = ByteBuffer.allocateDirect(ev.componentSize * size)
        new BufferWrapper[A, B](b, ev.wrapBuffer(b))
      }
    }
    def apply[A] = new TypedBufferFactory[A]
  }
  // class AnyValUtils[A <: AnyVal] {
  //   def createBuffer
  //       [B <: java.nio.Buffer]
  //       (size: Int)
  //       (implicit ev: CanBuildDirectBuffer[A, B])
  //       : BufferWrapper[B] =
  //     ev.createBuffer(size)
  // }
  // def anyVal[A] = new {
  //   def createBuffer
  // }

  // def typeTag[A: TypeTag]: TypeTag[A] = implicitly[TypeTag[A]]

  // def newBuffer
  //     [A, B <: java.nio.Buffer]
  //     (a: TypeTag[A], length: Int)
  //     (implicit ev: HasBuffer[A, B]): BufferWrapper[B] = {
  //   import definitions._
  //   (implicitly[TypeTag[A]].tpe match {
  //     case IntTpe => ByteBuffer.allocateDirect(4 * length).asIntBuffer()
  //     case LongTpe => ByteBuffer.allocateDirect(8 * length).asLongBuffer()
  //     case ShortTpe => ByteBuffer.allocateDirect(2 * length).asShortBuffer()
  //     case CharTpe => ByteBuffer.allocateDirect(2 * length).asCharBuffer()
  //     case ByteTpe => ByteBuffer.allocateDirect(length)
  //     case DoubleTpe => ByteBuffer.allocateDirect(8 * length).asDoubleBuffer()
  //     case FloatTpe => ByteBuffer.allocateDirect(4 * length).asFloatBuffer()
  //   }).asInstanceOf[B]
  // }
  // def newBuffer
  //     [A, B <: java.nio.Buffer]
  //     (elementClassTag: ClassTag[A], size: Int)
  //     (implicit ev: CanBuildDirectBuffer[A, B]): BufferWrapper[B] = {
  //   new BufferWrapper(ev.createBuffer(size))
  // }

  class UserBuffer(size: Int) extends User[TypedBuffer] {
    //    override val name: BufferWrapper = new scala.Array[String](size)
    //    override val birthDate = new scala.Array[Date](size)
    override val kudosCount = TypedBuffer[Int](size)
    override val a = TypedBuffer[Double](size)
    override val b = TypedBuffer[Double](size)
    override val c = TypedBuffer[Double](size)
    override val d = TypedBuffer[Double](size)
    override val e = TypedBuffer[Double](size)
    override val f = TypedBuffer[Double](size)
    override val g = TypedBuffer[Double](size)
    override val h = TypedBuffer[Double](size)
    override val i = TypedBuffer[Double](size)
    override val j = TypedBuffer[Double](size)
    override val k = TypedBuffer[Double](size)
    override val l = TypedBuffer[Double](size)
    //  val address = new AddressArray(size)
  }

  object UnsafePointer {
    import sun.misc.Unsafe
    //private[UnsafePointer] 
    val unsafe = {
      try {
        Unsafe.getUnsafe
      } catch {
        case ex: SecurityException =>
          val c = classOf[Unsafe]
          val f = c.getDeclaredField("theUnsafe")
          f.setAccessible(true)
          f.get(null).asInstanceOf[Unsafe]
      }
    }

    def allocate[A](size: Long): UnsafePointer[A] = new UnsafePointer[A](unsafe.allocateMemory(size))
  }
  case class UnsafePointer[A](ptr: Long) extends AnyVal {
    import UnsafePointer._

    def getIntAtIndex(index: Long): Int = unsafe.getInt(ptr + (index << 2))
    def putIntAtIndex(index: Long, value: Int): Unit = unsafe.putInt(ptr + (index << 2), value)
    def getShortAtIndex(index: Long): Short = unsafe.getShort(ptr + (index << 1))
    def putShortAtIndex(index: Long, value: Short): Unit = unsafe.putShort(ptr + (index << 1), value)
    def getLongAtIndex(index: Long): Long = unsafe.getLong(ptr + (index << 3))
    def putLongAtIndex(index: Long, value: Long): Unit = unsafe.putLong(ptr + (index << 3), value)
    def getByteAtIndex(index: Long): Byte = unsafe.getByte(ptr + index)
    def putByteAtIndex(index: Long, value: Byte): Unit = unsafe.putByte(ptr + index, value)
    def getBooleanAtIndex(index: Long): Boolean = getByteAtIndex(index) != 0
    def putBooleanAtIndex(index: Long, value: Boolean): Unit = putByteAtIndex(index, if (value) 1 else 0)
    def getCharAtIndex(index: Long): Char = unsafe.getChar(ptr + (index << 1))
    def putCharAtIndex(index: Long, value: Char): Unit = unsafe.putChar(ptr + (index << 1), value)
    def getFloatAtIndex(index: Long): Float = unsafe.getFloat(ptr + (index << 2))
    def putFloatAtIndex(index: Long, value: Float): Unit = unsafe.putFloat(ptr + (index << 2), value)
    def getDoubleAtIndex(index: Long): Double = unsafe.getDouble(ptr + (index << 3))
    def putDoubleAtIndex(index: Long, value: Double): Unit = unsafe.putDouble(ptr + (index << 3), value)
  }
  class UserUnsafePointer(size: Int) extends User[UnsafePointer] {
    //    override val name: BufferWrapper = new scala.Array[String](size)
    //    override val birthDate = new scala.Array[Date](size)
    override val kudosCount = UnsafePointer.allocate[Int](4 * size)
    override val a = UnsafePointer.allocate[Double](8 * size)
    override val b = UnsafePointer.allocate[Double](8 * size)
    override val c = UnsafePointer.allocate[Double](8 * size)
    override val d = UnsafePointer.allocate[Double](8 * size)
    override val e = UnsafePointer.allocate[Double](8 * size)
    override val f = UnsafePointer.allocate[Double](8 * size)
    override val g = UnsafePointer.allocate[Double](8 * size)
    override val h = UnsafePointer.allocate[Double](8 * size)
    override val i = UnsafePointer.allocate[Double](8 * size)
    override val j = UnsafePointer.allocate[Double](8 * size)
    override val k = UnsafePointer.allocate[Double](8 * size)
    override val l = UnsafePointer.allocate[Double](8 * size)
    //  val address = new AddressArray(size)
    override def finalize() = {
      UnsafePointer.unsafe.freeMemory(kudosCount.ptr)
      UnsafePointer.unsafe.freeMemory(a.ptr)
      UnsafePointer.unsafe.freeMemory(b.ptr)
      UnsafePointer.unsafe.freeMemory(c.ptr)
      UnsafePointer.unsafe.freeMemory(d.ptr)
      UnsafePointer.unsafe.freeMemory(e.ptr)
      UnsafePointer.unsafe.freeMemory(f.ptr)
      UnsafePointer.unsafe.freeMemory(g.ptr)
      UnsafePointer.unsafe.freeMemory(h.ptr)
      UnsafePointer.unsafe.freeMemory(i.ptr)
      UnsafePointer.unsafe.freeMemory(j.ptr)
      UnsafePointer.unsafe.freeMemory(k.ptr)
      UnsafePointer.unsafe.freeMemory(l.ptr)
    }
  }
  class UserUnsafeInterleavedPointer(size: Int) extends User[UnsafePointer] {
    private[this] val ptr = UnsafePointer.unsafe.allocateMemory(
      4 * size +
      8 * size +
      8 * size +
      8 * size +
      8 * size +
      8 * size +
      8 * size +
      8 * size +
      8 * size +
      8 * size +
      8 * size +
      8 * size +
      8 * size
    )
    private[this] var currentOffset = 0L
    private[this] def next[A](componentSize: Long): UnsafePointer[A] = {
      val length = componentSize * size
      val p = new UnsafePointer[A](ptr + currentOffset)
      currentOffset += length
      p
    }
    //    override val name: BufferWrapper = new scala.Array[String](size)
    //    override val birthDate = new scala.Array[Date](size)
    override val kudosCount = next[Int](4)
    override val a = next[Double](8)
    override val b = next[Double](8)
    override val c = next[Double](8)
    override val d = next[Double](8)
    override val e = next[Double](8)
    override val f = next[Double](8)
    override val g = next[Double](8)
    override val h = next[Double](8)
    override val i = next[Double](8)
    override val j = next[Double](8)
    override val k = next[Double](8)
    override val l = next[Double](8)
    //  val address = new AddressArray(size)
    override def finalize() = {
      UnsafePointer.unsafe.freeMemory(ptr)
    }
  }

  // Note: making this final degrates performance to the level of the generated cursor.
  // Really not sure why, needs investigation
  class UserUnsafePointerCursor(buffer: UserUnsafePointer, val length: Int) extends User[Accessors] with CursorLike {//Record[User]#Cursor {
  // override val name = new Accessors[String] {
  //   @inline def apply() = array.name(row)
  //   @inline def update(value: String) = array.name(row) = value
  // }
  // override val birthDate = new Accessors[Date] {
  //   @inline def apply() = array.birthDate(row)
  //   @inline def update(value: Date) = array.birthDate(row) = value
  // }
  override val kudosCount = new Accessors[Int] {
      private[this] val kudosCount = buffer.kudosCount
      private[this] val unsafe = UnsafePointer.unsafe
      @inline def apply() = unsafe.getInt(kudosCount.ptr + (row << 2))
      @inline def update(value: Int) = unsafe.putInt(kudosCount.ptr + (row << 2), value)
    }
    override val a = new Accessors[Double] {
      private[this] val a = buffer.a
      private[this] val unsafe = UnsafePointer.unsafe
      @inline def apply() = unsafe.getDouble(a.ptr + (row << 3))
      @inline def update(value: Double) = unsafe.putDouble(a.ptr + (row << 3), value)
    }
    override val b = new Accessors[Double] {
      private[this] val b = buffer.b
      private[this] val unsafe = UnsafePointer.unsafe
      @inline def apply() = unsafe.getDouble(b.ptr + (row << 3))
      @inline def update(value: Double) = unsafe.putDouble(b.ptr + (row << 3), value)
    }
    override val c = new Accessors[Double] {
      private[this] val c = buffer.c
      private[this] val unsafe = UnsafePointer.unsafe
      @inline def apply() = unsafe.getDouble(c.ptr + (row << 3))
      @inline def update(value: Double) = unsafe.putDouble(c.ptr + (row << 3), value)
    }
    override val d = new Accessors[Double] {
      private[this] val d = buffer.d
      private[this] val unsafe = UnsafePointer.unsafe
      @inline def apply() = unsafe.getDouble(d.ptr + (row << 3))
      @inline def update(value: Double) = unsafe.putDouble(d.ptr + (row << 3), value)
    }
    override val e = new Accessors[Double] {
      private[this] val e = buffer.e
      private[this] val unsafe = UnsafePointer.unsafe
      @inline def apply() = unsafe.getDouble(e.ptr + (row << 3))
      @inline def update(value: Double) = unsafe.putDouble(e.ptr + (row << 3), value)
    }
    override val f = new Accessors[Double] {
      private[this] val f = buffer.f
      private[this] val unsafe = UnsafePointer.unsafe
      @inline def apply() = unsafe.getDouble(f.ptr + (row << 3))
      @inline def update(value: Double) = unsafe.putDouble(f.ptr + (row << 3), value)
    }
    override val g = new Accessors[Double] {
      private[this] val g = buffer.g
      private[this] val unsafe = UnsafePointer.unsafe
      @inline def apply() = unsafe.getDouble(g.ptr + (row << 3))
      @inline def update(value: Double) = unsafe.putDouble(g.ptr + (row << 3), value)
    }
    override val h = new Accessors[Double] {
      private[this] val h = buffer.h
      private[this] val unsafe = UnsafePointer.unsafe
      @inline def apply() = unsafe.getDouble(h.ptr + (row << 3))
      @inline def update(value: Double) = unsafe.putDouble(h.ptr + (row << 3), value)
    }
    override val i = new Accessors[Double] {
      private[this] val i = buffer.i
      private[this] val unsafe = UnsafePointer.unsafe
      @inline def apply() = unsafe.getDouble(i.ptr + (row << 3))
      @inline def update(value: Double) = unsafe.putDouble(i.ptr + (row << 3), value)
    }
    override val j = new Accessors[Double] {
      private[this] val j = buffer.j
      private[this] val unsafe = UnsafePointer.unsafe
      @inline def apply() = unsafe.getDouble(j.ptr + (row << 3))
      @inline def update(value: Double) = unsafe.putDouble(j.ptr + (row << 3), value)
    }
    override val k = new Accessors[Double] {
      private[this] val k = buffer.k
      private[this] val unsafe = UnsafePointer.unsafe
      @inline def apply() = unsafe.getDouble(k.ptr + (row << 3))
      @inline def update(value: Double) = unsafe.putDouble(k.ptr + (row << 3), value)
    }
    override val l = new Accessors[Double] {
      private[this] val l = buffer.l
      private[this] val unsafe = UnsafePointer.unsafe
      @inline def apply() = unsafe.getDouble(l.ptr + (row << 3))
      @inline def update(value: Double) = unsafe.putDouble(l.ptr + (row << 3), value)
    }
  }

  // Note: making this final degrates performance to the level of the generated cursor.
  // Really not sure why, needs investigation
  class UserUnsafeInterleavedPointerCursor(buffer: UserUnsafeInterleavedPointer, val length: Int) extends User[Accessors] with CursorLike {//Record[User]#Cursor {
  // override val name = new Accessors[String] {
  //   @inline def apply() = array.name(row)
  //   @inline def update(value: String) = array.name(row) = value
  // }
  // override val birthDate = new Accessors[Date] {
  //   @inline def apply() = array.birthDate(row)
  //   @inline def update(value: Date) = array.birthDate(row) = value
  // }
  override val kudosCount = new Accessors[Int] {
      private[this] val kudosCount = buffer.kudosCount
      private[this] val unsafe = UnsafePointer.unsafe
      @inline def apply() = unsafe.getInt(kudosCount.ptr + (row << 2))
      @inline def update(value: Int) = unsafe.putInt(kudosCount.ptr + (row << 2), value)
    }
    override val a = new Accessors[Double] {
      private[this] val a = buffer.a
      private[this] val unsafe = UnsafePointer.unsafe
      @inline def apply() = unsafe.getDouble(a.ptr + (row << 3))
      @inline def update(value: Double) = unsafe.putDouble(a.ptr + (row << 3), value)
    }
    override val b = new Accessors[Double] {
      private[this] val b = buffer.b
      private[this] val unsafe = UnsafePointer.unsafe
      @inline def apply() = unsafe.getDouble(b.ptr + (row << 3))
      @inline def update(value: Double) = unsafe.putDouble(b.ptr + (row << 3), value)
    }
    override val c = new Accessors[Double] {
      private[this] val c = buffer.c
      private[this] val unsafe = UnsafePointer.unsafe
      @inline def apply() = unsafe.getDouble(c.ptr + (row << 3))
      @inline def update(value: Double) = unsafe.putDouble(c.ptr + (row << 3), value)
    }
    override val d = new Accessors[Double] {
      private[this] val d = buffer.d
      private[this] val unsafe = UnsafePointer.unsafe
      @inline def apply() = unsafe.getDouble(d.ptr + (row << 3))
      @inline def update(value: Double) = unsafe.putDouble(d.ptr + (row << 3), value)
    }
    override val e = new Accessors[Double] {
      private[this] val e = buffer.e
      private[this] val unsafe = UnsafePointer.unsafe
      @inline def apply() = unsafe.getDouble(e.ptr + (row << 3))
      @inline def update(value: Double) = unsafe.putDouble(e.ptr + (row << 3), value)
    }
    override val f = new Accessors[Double] {
      private[this] val f = buffer.f
      private[this] val unsafe = UnsafePointer.unsafe
      @inline def apply() = unsafe.getDouble(f.ptr + (row << 3))
      @inline def update(value: Double) = unsafe.putDouble(f.ptr + (row << 3), value)
    }
    override val g = new Accessors[Double] {
      private[this] val g = buffer.g
      private[this] val unsafe = UnsafePointer.unsafe
      @inline def apply() = unsafe.getDouble(g.ptr + (row << 3))
      @inline def update(value: Double) = unsafe.putDouble(g.ptr + (row << 3), value)
    }
    override val h = new Accessors[Double] {
      private[this] val h = buffer.h
      private[this] val unsafe = UnsafePointer.unsafe
      @inline def apply() = unsafe.getDouble(h.ptr + (row << 3))
      @inline def update(value: Double) = unsafe.putDouble(h.ptr + (row << 3), value)
    }
    override val i = new Accessors[Double] {
      private[this] val i = buffer.i
      private[this] val unsafe = UnsafePointer.unsafe
      @inline def apply() = unsafe.getDouble(i.ptr + (row << 3))
      @inline def update(value: Double) = unsafe.putDouble(i.ptr + (row << 3), value)
    }
    override val j = new Accessors[Double] {
      private[this] val j = buffer.j
      private[this] val unsafe = UnsafePointer.unsafe
      @inline def apply() = unsafe.getDouble(j.ptr + (row << 3))
      @inline def update(value: Double) = unsafe.putDouble(j.ptr + (row << 3), value)
    }
    override val k = new Accessors[Double] {
      private[this] val k = buffer.k
      private[this] val unsafe = UnsafePointer.unsafe
      @inline def apply() = unsafe.getDouble(k.ptr + (row << 3))
      @inline def update(value: Double) = unsafe.putDouble(k.ptr + (row << 3), value)
    }
    override val l = new Accessors[Double] {
      private[this] val l = buffer.l
      private[this] val unsafe = UnsafePointer.unsafe
      @inline def apply() = unsafe.getDouble(l.ptr + (row << 3))
      @inline def update(value: Double) = unsafe.putDouble(l.ptr + (row << 3), value)
    }
  }

  // Note: making this final degrates performance to the level of the generated cursor.
  // Really not sure why, needs investigation
  class UserTypedBufferCursor(buffer: UserBuffer, val length: Int) extends User[Accessors] with CursorLike {//Record[User]#Cursor {
    // override val name = new Accessors[String] {
    //   @inline def apply() = array.name(row)
    //   @inline def update(value: String) = array.name(row) = value
    // }
    // override val birthDate = new Accessors[Date] {
    //   @inline def apply() = array.birthDate(row)
    //   @inline def update(value: Date) = array.birthDate(row) = value
    // }
    override val kudosCount = new Accessors[Int] {
      private[this] val kudosCount = buffer.kudosCount.typedBuffer
      @inline def apply() = kudosCount.get(row)
      @inline def update(value: Int) = kudosCount.put(row, value)
    }
    override val a = new Accessors[Double] {
      private[this] val a = buffer.a.typedBuffer
      @inline def apply() = a.get(row)
      @inline def update(value: Double) = a.put(row, value)
    }
    override val b = new Accessors[Double] {
      private[this] val b = buffer.b.typedBuffer
      @inline def apply() = b.get(row)
      @inline def update(value: Double) = b.put(row, value)
    }
    override val c = new Accessors[Double] {
      private[this] val c = buffer.c.typedBuffer
      @inline def apply() = c.get(row)
      @inline def update(value: Double) = c.put(row, value)
    }
    override val d = new Accessors[Double] {
      private[this] val d = buffer.d.typedBuffer
      @inline def apply() = d.get(row)
      @inline def update(value: Double) = d.put(row, value)
    }
    override val e = new Accessors[Double] {
      private[this] val e = buffer.e.typedBuffer
      @inline def apply() = e.get(row)
      @inline def update(value: Double) = e.put(row, value)
    }
    override val f = new Accessors[Double] {
      private[this] val f = buffer.f.typedBuffer
      @inline def apply() = f.get(row)
      @inline def update(value: Double) = f.put(row, value)
    }
    override val g = new Accessors[Double] {
      private[this] val g = buffer.g.typedBuffer
      @inline def apply() = g.get(row)
      @inline def update(value: Double) = g.put(row, value)
    }
    override val h = new Accessors[Double] {
      private[this] val h = buffer.h.typedBuffer
      @inline def apply() = h.get(row)
      @inline def update(value: Double) = h.put(row, value)
    }
    override val i = new Accessors[Double] {
      private[this] val i = buffer.i.typedBuffer
      @inline def apply() = i.get(row)
      @inline def update(value: Double) = i.put(row, value)
    }
    override val j = new Accessors[Double] {
      private[this] val j = buffer.j.typedBuffer
      @inline def apply() = j.get(row)
      @inline def update(value: Double) = j.put(row, value)
    }
    override val k = new Accessors[Double] {
      private[this] val k = buffer.k.typedBuffer
      @inline def apply() = k.get(row)
      @inline def update(value: Double) = k.put(row, value)
    }
    override val l = new Accessors[Double] {
      private[this] val l = buffer.l.typedBuffer
      @inline def apply() = l.get(row)
      @inline def update(value: Double) = l.put(row, value)
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

  // Note: making this final degrates performance to the level of the generated cursor.
  // Really not sure why, needs investigation
  class UserByteBufferCursor(buffer: UserBuffer, val length: Int) extends User[Accessors] with CursorLike {//Record[User]#Cursor {
    // override val name = new Accessors[String] {
    //   @inline def apply() = array.name(row)
    //   @inline def update(value: String) = array.name(row) = value
    // }
    // override val birthDate = new Accessors[Date] {
    //   @inline def apply() = array.birthDate(row)
    //   @inline def update(value: Date) = array.birthDate(row) = value
    // }
    override val kudosCount = new Accessors[Int] {
      private[this] val kudosCount = buffer.kudosCount.buffer
      @inline def apply() = kudosCount.getInt(row << 2)
      @inline def update(value: Int) = kudosCount.putInt(row << 2, value)
    }
    override val a = new Accessors[Double] {
      private[this] val a = buffer.a.buffer
      @inline def apply() = a.getDouble(row << 3)
      @inline def update(value: Double) = a.putDouble(row << 3, value)
    }
    override val b = new Accessors[Double] {
      private[this] val b = buffer.b.buffer
      @inline def apply() = b.getDouble(row << 3)
      @inline def update(value: Double) = b.putDouble(row << 3, value)
    }
    override val c = new Accessors[Double] {
      private[this] val c = buffer.c.buffer
      @inline def apply() = c.getDouble(row << 3)
      @inline def update(value: Double) = c.putDouble(row << 3, value)
    }
    override val d = new Accessors[Double] {
      private[this] val d = buffer.d.buffer
      @inline def apply() = d.getDouble(row << 3)
      @inline def update(value: Double) = d.putDouble(row << 3, value)
    }
    override val e = new Accessors[Double] {
      private[this] val e = buffer.e.buffer
      @inline def apply() = e.getDouble(row << 3)
      @inline def update(value: Double) = e.putDouble(row << 3, value)
    }
    override val f = new Accessors[Double] {
      private[this] val f = buffer.f.buffer
      @inline def apply() = f.getDouble(row << 3)
      @inline def update(value: Double) = f.putDouble(row << 3, value)
    }
    override val g = new Accessors[Double] {
      private[this] val g = buffer.g.buffer
      @inline def apply() = g.getDouble(row << 3)
      @inline def update(value: Double) = g.putDouble(row << 3, value)
    }
    override val h = new Accessors[Double] {
      private[this] val h = buffer.h.buffer
      @inline def apply() = h.getDouble(row << 3)
      @inline def update(value: Double) = h.putDouble(row << 3, value)
    }
    override val i = new Accessors[Double] {
      private[this] val i = buffer.i.buffer
      @inline def apply() = i.getDouble(row << 3)
      @inline def update(value: Double) = i.putDouble(row << 3, value)
    }
    override val j = new Accessors[Double] {
      private[this] val j = buffer.j.buffer
      @inline def apply() = j.getDouble(row << 3)
      @inline def update(value: Double) = j.putDouble(row << 3, value)
    }
    override val k = new Accessors[Double] {
      private[this] val k = buffer.k.buffer
      @inline def apply() = k.getDouble(row << 3)
      @inline def update(value: Double) = k.putDouble(row << 3, value)
    }
    override val l = new Accessors[Double] {
      private[this] val l = buffer.l.buffer
      @inline def apply() = l.getDouble(row << 3)
      @inline def update(value: Double) = l.putDouble(row << 3, value)
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

// Note: making this final degrates performance to the level of the generated cursor.
// Really not sure why, needs investigation
class UserCursor(array: Record[User]#Array, val length: Int) extends User[Accessors] with CursorLike {//Record[User]#Cursor {
  // override val name = new Accessors[String] {
  //   @inline def apply() = array.name(row)
  //   @inline def update(value: String) = array.name(row) = value
  // }
  // override val birthDate = new Accessors[Date] {
  //   @inline def apply() = array.birthDate(row)
  //   @inline def update(value: Date) = array.birthDate(row) = value
  // }
  override val kudosCount = new Accessors[Int] {
    @inline def apply() = array.kudosCount(row)
    @inline def update(value: Int) = array.kudosCount(row) = value
  }
  override val a = new Accessors[Double] {
    @inline def apply() = array.a(row)
    @inline def update(value: Double) = array.a(row) = value
  }
  override val b = new Accessors[Double] {
    @inline def apply() = array.b(row)
    @inline def update(value: Double) = array.b(row) = value
  }
  override val c = new Accessors[Double] {
    @inline def apply() = array.c(row)
    @inline def update(value: Double) = array.c(row) = value
  }
  override val d = new Accessors[Double] {
    @inline def apply() = array.d(row)
    @inline def update(value: Double) = array.d(row) = value
  }
  override val e = new Accessors[Double] {
    @inline def apply() = array.e(row)
    @inline def update(value: Double) = array.e(row) = value
  }
  override val f = new Accessors[Double] {
    @inline def apply() = array.f(row)
    @inline def update(value: Double) = array.f(row) = value
  }
  override val g = new Accessors[Double] {
    @inline def apply() = array.g(row)
    @inline def update(value: Double) = array.g(row) = value
  }
  override val h = new Accessors[Double] {
    @inline def apply() = array.h(row)
    @inline def update(value: Double) = array.h(row) = value
  }
  override val i = new Accessors[Double] {
    @inline def apply() = array.i(row)
    @inline def update(value: Double) = array.i(row) = value
  }
  override val j = new Accessors[Double] {
    @inline def apply() = array.j(row)
    @inline def update(value: Double) = array.j(row) = value
  }
  override val k = new Accessors[Double] {
    @inline def apply() = array.k(row)
    @inline def update(value: Double) = array.k(row) = value
  }
  override val l = new Accessors[Double] {
    @inline def apply() = array.l(row)
    @inline def update(value: Double) = array.l(row) = value
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
final class FinalUserCursor(array: Record[User]#Array, val length: Int) extends User[Accessors] with CursorLike {//Record[User]#Cursor {
  // override val name = new Accessors[String] {
  //   @inline def apply() = array.name(row)
  //   @inline def update(value: String) = array.name(row) = value
  // }
  // override val birthDate = new Accessors[Date] {
  //   @inline def apply() = array.birthDate(row)
  //   @inline def update(value: Date) = array.birthDate(row) = value
  // }
  override val kudosCount = new Accessors[Int] {
    @inline def apply() = array.kudosCount(row)
    @inline def update(value: Int) = array.kudosCount(row) = value
  }
  override val a = new Accessors[Double] {
    @inline def apply() = array.a(row)
    @inline def update(value: Double) = array.a(row) = value
  }
  override val b = new Accessors[Double] {
    @inline def apply() = array.b(row)
    @inline def update(value: Double) = array.b(row) = value
  }
  override val c = new Accessors[Double] {
    @inline def apply() = array.c(row)
    @inline def update(value: Double) = array.c(row) = value
  }
  override val d = new Accessors[Double] {
    @inline def apply() = array.d(row)
    @inline def update(value: Double) = array.d(row) = value
  }
  override val e = new Accessors[Double] {
    @inline def apply() = array.e(row)
    @inline def update(value: Double) = array.e(row) = value
  }
  override val f = new Accessors[Double] {
    @inline def apply() = array.f(row)
    @inline def update(value: Double) = array.f(row) = value
  }
  override val g = new Accessors[Double] {
    @inline def apply() = array.g(row)
    @inline def update(value: Double) = array.g(row) = value
  }
  override val h = new Accessors[Double] {
    @inline def apply() = array.h(row)
    @inline def update(value: Double) = array.h(row) = value
  }
  override val i = new Accessors[Double] {
    @inline def apply() = array.i(row)
    @inline def update(value: Double) = array.i(row) = value
  }
  override val j = new Accessors[Double] {
    @inline def apply() = array.j(row)
    @inline def update(value: Double) = array.j(row) = value
  }
  override val k = new Accessors[Double] {
    @inline def apply() = array.k(row)
    @inline def update(value: Double) = array.k(row) = value
  }
  override val l = new Accessors[Double] {
    @inline def apply() = array.l(row)
    @inline def update(value: Double) = array.l(row) = value
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

abstract class CachedUserCursor[R](val length: Int, records: scala.Array[R], array: Record[User]#Array, io: Record[User]#IO[R]) extends User[Accessors] with CursorLike {//Record[User]#Cursor {
  private[this] val accessedFieldsArray = new scala.Array[Int](length)
  private[this] val currentAccessedFields = new IntRef(0)
  override def setRow(value: Int) {
    super.setRow(value)
    currentAccessedFields.elem = if (value < 0) 0 else accessedFieldsArray(value)
  }

  // val name = new CachedAccessor[String](1 << 0, currentAccessedFields) {
  //   def apply() = {
  //     if (isFirstAccess) this() = io.name.read(records(row))
  //     array.name(row)
  //   }
  //   def update(value: String) = array.name(row) = value
  // }
  // val birthDate = new CachedAccessor[Date](1 << 1, currentAccessedFields) {
  //   def apply() = {
  //     if (isFirstAccess) this() = io.birthDate.read(records(row))
  //     array.birthDate(row)
  //   }
  //   def update(value: Date) = array.birthDate(row) = value
  // }
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

    val n = 1000000
    val loops = 10
    val times = 30
    val warmups = 10

    val cursorRunner = new CursorRunner(n, loops)
    val typedBufferCursorRunner = new TypedBufferCursorRunner(n, loops)
    val byteBufferCursorRunner = new ByteBufferCursorRunner(n, loops)
    val cursorFactoryRunner = new CursorFactoryRunner(n, loops)
    val cursorCustomFactoryRunner = new CursorCustomFactoryRunner(n, loops)
    val arrayRunner = new ArrayRunner(n, loops)
    val customCursorRunner = new CustomCursorRunner(n, loops)
    val customFinalCursorRunner = new CustomFinalCursorRunner(n, loops)
    val unsafeCursorRunner = new UnsafeCursorRunner(n, loops)
    val unsafeInterleavedCursorRunner = new UnsafeInterleavedCursorRunner(n, loops)

    val timers = new Timers

    for (i <- 0 until (warmups + times)) {
      if (i == warmups) timers.clear()
      val values = Seq(
        timers.time("generated cursor") {
          cursorRunner.sum
        },
        timers.time("generated cursor factory") {
          cursorFactoryRunner.sum
        },
        timers.time("custom typed buffer cursor") {
          typedBufferCursorRunner.sum
        },
        timers.time("custom byte buffer cursor") {
          byteBufferCursorRunner.sum
        },
        timers.time("custom cursor (final)") {
          customFinalCursorRunner.sum
        },
        timers.time("custom cursor") {
          customCursorRunner.sum
        },
        timers.time("custom unsafe pointer cursor") {
          unsafeCursorRunner.sum
        },
        timers.time("custom unsafe interleaved pointer cursor") {
          unsafeInterleavedCursorRunner.sum
        },

        timers.time("generated cursor with custom factory") {
          cursorCustomFactoryRunner.sum
        },

        timers.time("array ") {
          arrayRunner.sum
        }
      )

      if (values.toSet.size != 1) sys.error(s"err: $values")
      println()
    }

    timers.printAverages()
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
