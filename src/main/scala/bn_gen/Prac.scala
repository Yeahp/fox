package bn_gen

/**
  * Created by erqi on 2017/7/27.
  */
import bn_gen.Element.elem
object Prac {
  def main(args: Array[String]): Unit = {
    val s = elem("hello")
    val f = elem(Array("we","are"))
    val d = elem("world!world")
    val k = s beside f
    println(k)
  }
}
