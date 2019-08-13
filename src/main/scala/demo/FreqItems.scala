package demo

import org.apache.spark.SparkConf
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.sql.SparkSession

object FreqItems {

  def main(args: Array[String]): Unit = {

    // parse arguments
    var dataPath = ""
    var resultPath = ""
    args.sliding(2, 2).toList.collect {
      case Array("--data-path", argDataPath: String) => dataPath = argDataPath
      case Array("--result-path", argResultPath: String) =>  resultPath = argResultPath
    }
    if (dataPath.length == 0 || resultPath.length == 0)
      throw new IllegalArgumentException("Wrong Required Arguments")

    // set spark configuration
    val sparkConf = new SparkConf().setAppName("frequet item mining").setMaster("local")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = sparkSession.sparkContext

    sc.makeRDD(List(1,2,3,4)).collect().foreach(println)

    // read data and mining pattern
    val data = sc.textFile(dataPath).map(s => s.trim.split("\t"))
    val fpg = new FPGrowth()
      .setMinSupport(0.2)
      .setNumPartitions(10)
    val model = fpg.run(data)

    // save result
    // val _result = model.freqItemsets.map {itemset => itemset.items.mkString("", "\t", "") + "," + itemset.freq}
    // result.saveAsTextFile(_resultPath)

    val minConfidence = 0.8
    val result = model.generateAssociationRules(minConfidence)
      .map(rule => rule.antecedent.mkString("", "\t", "") + "|" + rule.consequent.mkString("\t") + "|" + rule.confidence)
      .filter(s => s.contains("combo"))
    result.saveAsTextFile(resultPath)

  }

}
