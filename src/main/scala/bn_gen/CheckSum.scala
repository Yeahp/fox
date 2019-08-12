package bn_gen

/**
  * Created by erqi on 2017/7/18.
  */
class CheckSum {
  private var sum = 0
  def add(b:Byte) {sum+=b}
  def checksum(): Int = ~(sum & 0xFF)+1
}

import scala.collection.mutable.Map
object CheckSum {
  private val cache = Map[String, Int]()
  def calculate(s: String): Int = {
    if (cache.contains(s)) {
      cache(s)
    }
    else {
      val acc = new CheckSum
      for (c <- s) {
        acc.add(c.toByte)
      }
      val cs = acc.checksum()
      cache += (s -> cs)
      cs
    }
  }
}