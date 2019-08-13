package demo

class Point( val x: Int, val y: Int)

trait Graph{

  def topLeft: Point
  def bottomRight: Point

  def leftX = topLeft.x
  def rightX = bottomRight.x
  def width = bottomRight.x - topLeft.x

}
//class Graphh(val topLeft: Point, val bottomRight: Point) extends Graph
//{
//  override val topLeft: Point = topleft
//  override val bottomRight: Point = bottom
//}

class Graphh(topleft: Point, bottomright: Point) extends Graph {
  override val topLeft: Point = topleft
  override val bottomRight: Point = bottomright
}

object Graphh {
  def main(args: Array[String]): Unit = {
    val etc = new Graphh(new Point(1,2), new Point(2,3))
    println(etc.width)
  }
}

