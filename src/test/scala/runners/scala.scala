package scalaxy.records
package test

import java.util.Date

final class CursorRunner(n: Int) {

  private[this] val userArrayFactory = recordArrayFactory[User]
//  private[this] val userCursorFactory = recordCursorFactory[User]
  //  private[this] val userGetters = recordGetters[User]

  private[this] val users: Record[User]#Array = userArrayFactory(n)

  {
    var i = 0
    val cursor = recordCursor[User](users, n)
    //    val cursor = new UserCursor(users, n)
    while (cursor.next()) {
      cursor.kudosCount() = i
      i += 1
    }
  }

  def sum: Long = {
    var total = 0L
    val cursor = recordCursor[User](users, n)
    //    val cursor = new UserCursor(users, n)
    while (cursor.next()) {
      total += cursor.kudosCount()
    }
    total
  }
}

final class CursorFactoryRunner(n: Int) {

  private[this] val userArrayFactory = recordArrayFactory[User]
  private[this] val userCursorFactory = recordCursorFactory[User]
  //  private[this] val userGetters = recordGetters[User]

  private[this] val users: Record[User]#Array = userArrayFactory(n)

  {
    var i = 0
    val cursor = userCursorFactory(users, n)
    //    val cursor = new UserCursor(users, n)
    while (cursor.next()) {
      cursor.kudosCount() = i
      i += 1
    }
  }

  def sum: Long = {
    var total = 0L
    val cursor = userCursorFactory(users, n)
    //    val cursor = new UserCursor(users, n)
    while (cursor.next()) {
      total += cursor.kudosCount()
    }
    total
  }
}

final class CustomCursorRunner(n: Int) {

  private[this] val userArrayFactory = recordArrayFactory[User]
  //  private[this] val userCursorFactory = (users: Record[User]#Array, size: Int) => new UserCursor(users, n)
  //  private[this] val userGetters = recordGetters[User]

  private[this] val users: Record[User]#Array = userArrayFactory(n)

  {
    var i = 0
    //    val cursor = userCursorFactory(users, n)
    val cursor = new UserCursor(users, n)
    while (cursor.next()) {
      cursor.kudosCount() = i
      i += 1
    }
  }

  def sum: Long = {
    var total = 0L
    //    val cursor = userCursorFactory(users, n)
    val cursor = new UserCursor(users, n)
    while (cursor.next()) {
      total += cursor.kudosCount()
    }
    total
  }
}
final class CustomFinalCursorRunner(n: Int) {

  private[this] val userArrayFactory = recordArrayFactory[User]
  //  private[this] val userCursorFactory = (users: Record[User]#Array, size: Int) => new UserCursor(users, n)
  //  private[this] val userGetters = recordGetters[User]

  private[this] val users: Record[User]#Array = userArrayFactory(n)

  {
    var i = 0
    //    val cursor = userCursorFactory(users, n)
    val cursor = new FinalUserCursor(users, n)
    while (cursor.next()) {
      cursor.kudosCount() = i
      i += 1
    }
  }

  def sum: Long = {
    var total = 0L
    //    val cursor = userCursorFactory(users, n)
    val cursor = new FinalUserCursor(users, n)
    while (cursor.next()) {
      total += cursor.kudosCount()
    }
    total
  }
}

final class ArrayRunner(n: Int) {

  case class User(name: String, birthDate: Date, kudosCount: Int)

  private[this] val users = new Array[User](n)

  {
    var i = 0
    while (i < n) {
      users(i) = User(name = null, birthDate = null, kudosCount = i)
      i += 1
    }
  }

  def sum: Long = {
    var total = 0L
    var i = 0
    while (i < n) {
      val user = users(i)
      total += user.kudosCount
      i += 1
    }
    total
  }
}
