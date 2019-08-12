package bn_gen

/**
  * Created by erqi on 2017/7/18.
  */
import bn_gen.CheckSum.calculate
object Summer {
  def main(args: Array[String]){
    for (arg <- args)
      println(arg + ": " + calculate(arg))
  }
}
