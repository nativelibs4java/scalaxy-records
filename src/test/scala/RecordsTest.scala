package scalaxy.records
package test

import java.util.Date

import org.scalatest.{ FlatSpecLike, Matchers }

import scala.language.higherKinds

trait User[C[_]] extends Record[User] {
  val name: C[String]
  val birthDate: C[Date]
  val kudosCount: C[Int]
}

class RecordArraysTest extends FlatSpecLike with Matchers {

  behavior of "RecordArrays"

  it should "work" in {

    val userFactory = recordFactory[User]
    val userGetters: Record[User]#Getters = recordGetters[User]

    val n = 10
    val users = userFactory(n)
    println("users: " + users)
    println("name: " + users.name.toSeq)
    println("birthDate: " + users.birthDate.toSeq)
    println("kudosCount: " + users.kudosCount.toSeq)
    for (i <- 0 until n) {
       val name = userGetters.name(users, i)
       println(s"i = $i, name = $name")
    }
  }
}
