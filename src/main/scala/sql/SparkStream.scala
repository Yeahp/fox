package sql

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStream {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Test Spark Stream")
    val ssc = new StreamingContext(sparkConf,Seconds(1))
    // in practice, a streamingcontext contains a sparkcontext
    // val sc = ssc.sparkContext
    // if there has existed a sparkcontext, streamingcontext can be built as
    // val ssc = new StreamingContext(sc, Second(1))

//    val lines = ssc.socketStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
//    // a detailed version
//    val words = lines.flatMap(s => s.split(" "))
//    val pairs = words.map(word => (word, 1))
//    val wordCount = pairs.reduceByKey((a,b) => a+b)

    /* a simpler version follows
    val wordCount = lines.flatMap(s => s.split(" "))
      .map(word => (word,1))
      .reduceByKey((a,b) => a+b)
    */
//    wordCount.print()
//    wordCount.saveAsTextFiles("path","txt") //pay attention to function saveAsTextFile("path")

    // val fileSystem = ssc.fileStream[classOf[Text], classOf[IntWritable], KeyValueTextInputFormat]("path")

    // here, we define an updateStateByKey
    def updateFunc(thisBatchVaules: Seq[Int], stateValues: Option[Int]): Option[Int] = {
      val currentValue = thisBatchVaules.foldLeft(0)((a,b)=>a+b)
      val previousValue = stateValues.getOrElse(0)
      Some(currentValue+previousValue)
    }

    // or we can write update function in value type
    val updateFunc2 = (thisBatchValues: Seq[Int], stateValues: Option[Int]) => {
      val currentValue = thisBatchValues.foldLeft(0)((a,b)=>a+b)
      val previousValue = stateValues.getOrElse(0)
      Some(currentValue+previousValue)
    }

//    val newWordCount = pairs.updateStateByKey[Int](updateFunc _)
//    val newWordCount = pairs.updateStateByKey[Int](updateFunc2 _)
//    ssc.start()
//    ssc.awaitTermination()
    // ssc.stop(stopSparkContext = false)
  }
}
