# Scalaxy/Records

This is an abstraction of how data records can be stored in arrays of columns rather than rows of objects.

```scala

import scalaxy.records._

trait User[C[_]] extends Record[User] {
  val name: C[String]
  val birthDate: C[Date]
  val kudosCount: C[Int]
}

// This is a `Int => Record[User]#Array`.
val userFactory: Record[User]#Factory = recordFactory[User]
/* Macro expands to: {
  (size: Int) =>
    new Record[User]#Array {
      val name = new Array[String](size)
      val birthDate: Array[Date]
      val kudosCount: Array[Int]
    }
*/

val userGetters: Record[User]#Getters = recordGetters[User]

val n = 10
val users = userFactory(n)
println("name column: " + users.name.toSeq)
println("birthDate column: " + users.birthDate.toSeq)
println("kudosCount column: " + users.kudosCount.toSeq)
for (i <- 0 until n) {
   val name = userGetters.name(users, i)
   println(s"i = $i, name = $name")
}
```