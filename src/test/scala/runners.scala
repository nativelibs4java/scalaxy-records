package scalaxy.records
package test

import java.util.Date

final class CursorRunner(n: Int, loops: Int) {

  private[this] val userArrayFactory = recordArrayFactory[User]
//  private[this] val userCursorFactory = recordCursorFactory[User]
  //  private[this] val userGetters = recordGetters[User]

  private[this] val users: Record[User]#Array = userArrayFactory(n)

  {
    var i = 0
    val cursor = recordCursor[User](users, n)
    while (cursor.next()) {
      cursor.kudosCount() = i
      cursor.a() = i
      cursor.i() = i
      i += 1
    }
  }

  def sum: Double = {
    var total = 0.0
    var iLoop = 0
    while (iLoop < loops) {
      val cursor = recordCursor[User](users, n)
      while (cursor.next()) {
        total += cursor.kudosCount()
        total += cursor.a()
        total += cursor.i()
      }
      iLoop += 1
    }
    total
  }
}

final class CursorFactoryRunner(n: Int, loops: Int) {

  private[this] val userArrayFactory = recordArrayFactory[User]
  private[this] val userCursorFactory = recordCursorFactory[User]

  private[this] val users: Record[User]#Array = userArrayFactory(n)

  {
    var i = 0
    val cursor = userCursorFactory(users, n)
    while (cursor.next()) {
      cursor.kudosCount() = i
      cursor.a() = i
      cursor.i() = i
      i += 1
    }
  }

  def sum: Double = {
    var total = 0.0
    var iLoop = 0
    while (iLoop < loops) {
      val cursor = userCursorFactory(users, n)
      while (cursor.next()) {
        total += cursor.kudosCount()
        total += cursor.a()
        total += cursor.i()
      }
      iLoop += 1
    }
    total
  }
}
final class CursorCustomFactoryRunner(n: Int, loops: Int) {

  private[this] val userArrayFactory = recordArrayFactory[User]
  private[this] val userCursorFactory = (users: Record[User]#Array, length: Int) => recordCursor[User](users, length)

  private[this] val users: Record[User]#Array = userArrayFactory(n)

  {
    var i = 0
    val cursor = userCursorFactory(users, n)
    while (cursor.next()) {
      cursor.kudosCount() = i
      cursor.a() = i
      cursor.i() = i
      i += 1
    }
  }

  def sum: Double = {
    var total = 0.0
    var iLoop = 0
    while (iLoop < loops) {
      val cursor = userCursorFactory(users, n)
      while (cursor.next()) {
        total += cursor.kudosCount()
        total += cursor.a()
        total += cursor.i()
      }
      iLoop += 1
    }
    total
  }
}

final class CustomCursorRunner(n: Int, loops: Int) {

  private[this] val userArrayFactory = recordArrayFactory[User]
  private[this] val users: Record[User]#Array = userArrayFactory(n)

  {
    var i = 0
    val cursor = new UserCursor(users, n)
    while (cursor.next()) {
      cursor.kudosCount() = i
      cursor.a() = i
      cursor.i() = i
      i += 1
    }
  }

  def sum: Double = {
    var total = 0.0
    var iLoop = 0
    while (iLoop < loops) {
      val cursor = new UserCursor(users, n)
      while (cursor.next()) {
        total += cursor.kudosCount()
        total += cursor.a()
        total += cursor.i()
      }
      iLoop += 1
    }
    total
  }
}
final class CustomFinalCursorRunner(n: Int, loops: Int) {

  private[this] val userArrayFactory = recordArrayFactory[User]

  private[this] val users: Record[User]#Array = userArrayFactory(n)

  {
    var i = 0
    val cursor = new FinalUserCursor(users, n)
    while (cursor.next()) {
      cursor.kudosCount() = i
      cursor.a() = i
      cursor.i() = i
      i += 1
    }
  }

  def sum: Double = {
    var total = 0.0
    var iLoop = 0
    while (iLoop < loops) {
      val cursor = new FinalUserCursor(users, n)
      while (cursor.next()) {
        total += cursor.kudosCount()
        total += cursor.a()
        total += cursor.i()
      }
      iLoop += 1
    }
    total
  }
}

final class ArrayRunner(n: Int, loops: Int) {

  case class User(
    name: String,
    birthDate: Date,
    kudosCount: Int,
    a: Double = 0.0,
    b: Double = 0.0,
    c: Double = 0.0,
    d: Double = 0.0,
    e: Double = 0.0,
    f: Double = 0.0,
    g: Double = 0.0,
    h: Double = 0.0,
    i: Double = 0.0,
    j: Double = 0.0,
    k: Double = 0.0,
    l: Double = 0.0
  )

  private[this] val users = new Array[User](n)

  {
    var i = 0
    while (i < n) {
      users(i) = User(name = null, birthDate = null, kudosCount = i, a = i, i = i)
      i += 1
    }
  }

  def sum: Double = {
    var total = 0.0
    var iLoop = 0
    while (iLoop < loops) {
      var i = 0
      while (i < n) {
        val user = users(i)
        total += user.kudosCount
        total += user.a
        total += user.i
        i += 1
      }
      iLoop += 1
    }
    total
  }
}


final class TypedBufferCursorRunner(n: Int, loops: Int) {
  import buffers._
  // private[this] val userArrayFactory = recordArrayFactory[User]
//  private[this] val userCursorFactory = recordCursorFactory[User]
  //  private[this] val userGetters = recordGetters[User]

  private[this] val users = new UserBuffer(n)

  {
    var i = 0
    val cursor = new UserTypedBufferCursor(users, n)
    while (cursor.next()) {
      cursor.kudosCount() = i
      cursor.a() = i
      cursor.i() = i
      i += 1
    }
  }

  def sum: Double = {
    var total = 0.0
    var iLoop = 0
    while (iLoop < loops) {
      val cursor = new UserTypedBufferCursor(users, n)
      while (cursor.next()) {
        total += cursor.kudosCount()
        total += cursor.a()
        total += cursor.i()
      }
      iLoop += 1
    }
    total
  }
}

final class ByteBufferCursorRunner(n: Int, loops: Int) {
  import buffers._
  // private[this] val userArrayFactory = recordArrayFactory[User]
//  private[this] val userCursorFactory = recordCursorFactory[User]
  //  private[this] val userGetters = recordGetters[User]

  private[this] val users = new UserBuffer(n)

  {
    var i = 0
    val cursor = new UserByteBufferCursor(users, n)
    while (cursor.next()) {
      cursor.kudosCount() = i
      cursor.a() = i
      cursor.i() = i
      i += 1
    }
  }

  def sum: Double = {
    var total = 0.0
    var iLoop = 0
    while (iLoop < loops) {
      val cursor = new UserByteBufferCursor(users, n)
      while (cursor.next()) {
        total += cursor.kudosCount()
        total += cursor.a()
        total += cursor.i()
      }
      iLoop += 1
    }
    total
  }
}


final class UnsafeCursorRunner(n: Int, loops: Int) {
  import buffers._
  // private[this] val userArrayFactory = recordArrayFactory[User]
  //  private[this] val userCursorFactory = recordCursorFactory[User]
  //  private[this] val userGetters = recordGetters[User]

  private[this] val users = new UserUnsafePointer(n)

  {
    var i = 0
    val cursor = new UserUnsafePointerCursor(users, n)
    while (cursor.next()) {
      cursor.kudosCount() = i
      cursor.a() = i
      cursor.i() = i
      i += 1
    }
  }

  def sum: Double = {
    var total = 0.0
    var iLoop = 0
    while (iLoop < loops) {
      val cursor = new UserUnsafePointerCursor(users, n)
      while (cursor.next()) {
        total += cursor.kudosCount()
        total += cursor.a()
        total += cursor.i()
      }
      iLoop += 1
    }
    total
  }
}
final class UnsafeInterleavedCursorRunner(n: Int, loops: Int) {
  import buffers._
  // private[this] val userArrayFactory = recordArrayFactory[User]
  //  private[this] val userCursorFactory = recordCursorFactory[User]
  //  private[this] val userGetters = recordGetters[User]

  private[this] val users = new UserUnsafeInterleavedPointer(n)

  {
    var i = 0
    val cursor = new UserUnsafeInterleavedPointerCursor(users, n)
    while (cursor.next()) {
      cursor.kudosCount() = i
      cursor.a() = i
      cursor.i() = i
      i += 1
    }
  }

  def sum: Double = {
    var total = 0.0
    var iLoop = 0
    while (iLoop < loops) {
      val cursor = new UserUnsafeInterleavedPointerCursor(users, n)
      while (cursor.next()) {
        total += cursor.kudosCount()
        total += cursor.a()
        total += cursor.i()
      }
      iLoop += 1
    }
    total
  }
}