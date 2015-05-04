package scalaxy.records

package object test {

  def time[V](name: String)(v: => V): V = {
    import System.nanoTime
    val start = nanoTime()
    val res = v
    val end = nanoTime()
    println(s"[$name] ${(end - start) / 1000 / 1000.0} ms")
    res
  }

}