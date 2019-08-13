package io

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.ShortTypeHints
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import org.json4s.jackson.Serialization

object ReadJsonFromTxt {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("read json file from text file")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    // 1 -- read json file and parse it into scala class
    implicit val formats = Serialization.formats(ShortTypeHints(List()))
    case class Person(name: String, age: Int)
    val input = sc.textFile(args(0))
    val ss = input.map(record => {
      try {
        parse(record).extract[Person] // return a Person class, .name & .age will return name and age
      }
      catch {
        case e: Exception => None
      }
    }
    )

    // 2 -- transform Json DSL to Json AST
//    val json = (
//      ("name" -> "Eric") ~
//        ("age" -> 25)
//    )
//    //or we have a simpler expression
//    val json_ = ("name" -> "Eric") ~ ("age" -> 25)
//    val s = compact(render(json))
  }
}
