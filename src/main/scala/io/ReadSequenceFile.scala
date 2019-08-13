package io

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.{SparkConf, SparkContext}

object ReadSequenceFile {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Spark Read SequenceFiles")
    val sc = new SparkContext(sparkConf)
    // create a sequencefile and write it to disk
    val seqFile = sc.parallelize(List(("mike",29),("neo",27),("eric",30)))
    seqFile.saveAsSequenceFile("path")

    // read sequencefile
    val seqData = sc.sequenceFile("path", classOf[Text],classOf[IntWritable],3)
      .map{ case(x, y) => (x.toString, y.get()) }

    // scala class map to org.apache.hadoop.io.Writable
    val classMap = Map(
      "Int" -> "IntWritable"
      ,"Double" -> "DoubleWritable"
      ,"Float" -> "FloatWritable"
      ,"String" -> "Text"
      ,"Bollean" -> "BooleanWritable"
      ,"Long" -> "LongWritable"
      ,"Array[Byte]" -> "BytesWritable"
      ,"List[T]" -> "ArrayWritable"
      ,"Array[T]" -> "ArrayWritable"
      ,"Map[A,B]" -> "MapWritable"
    )

    // about object file
    val objFile = sc.objectFile("path")
    objFile.saveAsObjectFile("path")

    // shared variables
    val s = sc.longAccumulator("we")
    //val t = sc.broadcast(Array(1,2,3,4))
    val t = sc.broadcast(List(1,2,3,4,5))
  }
}
