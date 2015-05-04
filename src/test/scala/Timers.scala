package scalaxy.records

class Samples {

  var sampleCount: Long = _
  var total: Long = _
  var min: Long = _
  var max: Long = _

  def clear(): Unit = {
    sampleCount = 0L
    total = 0L
    min = Long.MaxValue
    max = Long.MinValue
  }
  clear()

  def addSample(time: Long): Unit = {
    sampleCount += 1
    total += time
    if (time < min) min = time
    if (time > max) max = time
  }

  def average = (total.toDouble / sampleCount)
}
class Timers {
  val map = collection.mutable.Map[String, Samples]()

  def time[V](name: String)(v: => V): V = {
    import System.nanoTime
    val start = nanoTime()
    val res = v
    val end = nanoTime()

    val elapsedMillis = (end - start) / 1000000

    println(s"[$name] ${elapsedMillis} ms")

    map.getOrElseUpdate(name, new Samples).addSample(elapsedMillis)

    res
  }

  def clear() = map.values.foreach(_.clear())

  def printAverages(): Unit = {
    for ((key, samples) <- map.toSeq.sortBy(_._1)) {
      val padding = key.map(_ => ' ') + "  "
      println(s"[$key] avg = ${samples.average} ms\n" +
              s"$padding min = ${samples.min} ms\n" +
              s"$padding max = ${samples.max} ms\n")
    }
  }
}