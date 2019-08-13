package demo

import org.apache.spark.{SparkConf, SparkContext}

object PMQueryProcess {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("PM Query Process")
    val sc = new SparkContext(sparkConf)

    // to avoid text error, pre-process data in linux shell
    /* sed -i "s/‘/'/g" pm_query.txt
       sed -i "s/’/'/g" pm_query.txt
       vim pm_query -> set ff = unix -> :wq
    */

    val data = sc.textFile("file:")
      .filter(!_.trim.equals("")) // exclude blank line
      .flatMap(_.split(" ")) // split line into words
      .map(_.toLowerCase) // to lower case, 1869 words
      .filter(!_.equals("")) // exclude blank character, 1852 words

    val data_ = data.map( s => {
      val len = s.length
      if (s.endsWith(",") || s.endsWith("?") || s.endsWith(".")) {
        s.substring(0, len-1)
      } else if (s.startsWith("'") && s.endsWith("'")) {
        s.substring(1,len-1)
      } else {
        s
      }
    })

    // for pattern match, using regex can achieve the same result
    val patternOne = "(')(.*)(')".r
    val patternTwo = "(.*)([.,?])".r
    val data__ = data.map { s => {
      s match {
        case patternOne(head,body,tail) => body
        case patternTwo(head,tail) => head
        case _ => s
      }
    }}

    val sortedWordCount = data_.map(word => (word,1))
      .reduceByKey((a, b) => a + b)  // 685 pairs
      .sortBy(_._2,false)
      .map{
        case(word, count) => word+ "\001" + count
      }

    sortedWordCount.repartition(1).saveAsTextFile("file:")
  }
}
