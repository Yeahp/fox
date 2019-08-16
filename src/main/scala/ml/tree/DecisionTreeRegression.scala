package ml.tree

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, RegressionMetrics}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD
import org.joda.time.{DateTime, Duration}

object DecisionTreeRegression {

  def main(args: Array[String]): Unit = {
    SetLogger
    val sc = new SparkContext(new SparkConf().setAppName("decision.tree.regression").setMaster("local[4]"))
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
      val label = trFields.last
      LabeledPoint(label, Vectors.dense(trFields.slice(0, trFields.length - 1)))
    }}
    val Array(trainData, validationData, testData)  = labelpointRDD.randomSplit(Array(0.8, 0.1, 0.1))
    (trainData, validationData, testData)
  }

  def trainEvaluate(trainData: RDD[LabeledPoint], validationData: RDD[LabeledPoint]): DecisionTreeModel = {
    println("========== START TRAINING MODEL ==========")
    val (model, time) = trainModel(trainData, "entropy", 10, 10)
    val rmse = evaluateModel(model, validationData)
    model
  }

  def trainModel(trainData: RDD[LabeledPoint], impurity: String, maxDepth: Int, maxBins: Int): (DecisionTreeModel, Double) = {
    val startTime = new DateTime()
    val model = DecisionTree.trainRegressor(trainData, Map[Int, Int](), impurity, maxDepth, maxBins)
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
    val metrics = new RegressionMetrics(scoreAndLabel)
    metrics.rootMeanSquaredError
  }

  def predictData(testData: RDD[LabeledPoint], model: DecisionTreeModel, categoriesMap: Map[String, Int]) = {

  }

}
