package bn_gen

/**
  * Created by erqi on 2017/7/27.
  */
object Element{
  private class ArrayElement(val contents: Array[String]) extends Element

  private class LineElement(s: String) extends Element{
    val contents = Array(s)
    override def width = s.length
    override def height = 1
  }

  private class UniformElement(ch: Char, override val width: Int, override val height: Int) extends Element{
    private val line = ch.toString * width
    def contents = {
      var num = 1
      var a = new Array[String](height)
      for (i <- 1 to height)
        a(i) = line
      a
    }
    // Here we apply a relatively complex algorithm to replace initial function Array.make(x: int, y: String)
    // The function Array.make(x: Int, y: String) turns out to be a litter trouble because it cannot work formally.
  }

  def elem(contents: Array[String]): Element = new ArrayElement(contents)

  def elem(chr: Char, width: Int, height: Int): Element = new UniformElement(chr, width, height)

  def elem(line: String): Element = new LineElement(line)
}

import bn_gen.Element.elem
abstract class Element {
  def contents: Array[String]
  def width: Int = contents(0).length
  def height: Int = contents.length

  def above(that: Element): Element = {
    val this1 = this widen that.width
    val that1 = that widen this.width
    elem(this1.contents ++ that1.contents)
  }

  def beside(that: Element): Element = {
    val this1 = this heighten that.height
    val that1 = that heighten this.height
    elem(for((line1, line2) <- this1.contents zip that1.contents) yield line1 + line2)
  }

  def widen(w: Int): Element =
    if (w <= width) this
    else {
      val left = elem( ' ', (w - width) / 2, height)
      val right = elem( ' ', w - width - left.width, height)
      left beside this beside right
    }

  def heighten(h: Int): Element =
    if (h <= height) this
    else {
      val top = elem(' ', width, (h - height) / 2)
      val bot = elem(' ', width, h - height - top.height)
      top above this above bot
    }

  override def toString = contents mkString "\n"
}