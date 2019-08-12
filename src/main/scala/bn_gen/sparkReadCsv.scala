package bn_gen

import org.apache.spark.sql.SparkSession

object sparkReadCsv {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("read csv file").getOrCreate()
    val data = spark.read.format("csv").options(Map("header" -> "true", "sep" -> ",")).load("C:/Users/erqi/Desktop/we.csv") // org.apache.spark.sql.DataFrame
    data.collect().foreach(println)
    data.write.format("csv").save("path")
    // data.write.csv("path")
    val sc = spark.sparkContext
    //sc.textFile()
  }
}
