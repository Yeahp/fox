package ml.tree

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD
import org.joda.time.{DateTime, Duration}

object DecisionTreeMulti {

  def main(args: Array[String]): Unit = {
    SetLogger
    val sc = new SparkContext(new SparkConf().setAppName("decision.tree.multi").setMaster("local[4]"))
  }

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger.setLevel(Level.OFF)
  }

  def prepareData(sc: SparkContext): (RDD[LabeledPoint], RDD[LabeledPoint], RDD[LabeledPoint]) = {
    val rawDataWithHeader = sc.textFile("file:")
    val rawData = rawDataWithHeader.mapPartitionsWithIndex {
      (idx, iter) => if (idx == 0) iter.drop(1) else iter
    }
    val lines = rawData.map(_.split("\t"))
    println("TOTAL RECORDS: " + lines.count())
    val labelpointRDD = lines.map { fields => {
      val trFields = fields.map(_.toDouble)
      val label = trFields.last - 1 // index based on 0
      LabeledPoint(label, Vectors.dense(trFields.init)) //.init take fields except the last column
    }}
    val Array(trainData, validationData, testData)  = labelpointRDD.randomSplit(Array(0.8, 0.1, 0.1))
    (trainData, validationData, testData)
  }

  def trainEvaluate(trainData: RDD[LabeledPoint], validationData: RDD[LabeledPoint]): DecisionTreeModel = {
    println("========== START TRAINING MODEL ==========")
    val (model, time) = trainModel(trainData, "entropy", 10, 10)
    val precision = evaluateModel(model, validationData)
    model
  }

  def trainModel(trainData: RDD[LabeledPoint], impurity: String, maxDepth: Int, maBins: Int): (DecisionTreeModel, Double) = {
    val startTime = new DateTime()
    val model = DecisionTree.trainClassifier(trainData, 7, Map[Int, Int](), impurity, maxDepth, maBins)
    val endTime = new DateTime()
    val duration = new Duration(startTime, endTime)
    (model, duration.getMillis)
  }

  def evaluateModel(model: DecisionTreeModel, validationData: RDD[LabeledPoint]): Double = {
    val scoreAndLabel = validationData.map {
      data => {
        var predict = model.predict(data.features)
        (predict, data.label)
      }
    }
    val metrics = new MulticlassMetrics(scoreAndLabel)
    metrics.precision
  }

  def predictData(testData: RDD[LabeledPoint], model: DecisionTreeModel, categoriesMap: Map[String, Int]) = {

  }

}
