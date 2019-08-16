package ml.nb

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel, SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.joda.time.{DateTime, Duration}

object NaiveBayesBinary {

  def main(args: Array[String]): Unit = {
    SetLogger
    val sc = new SparkContext(new SparkConf().setAppName("nb.binary").setMaster("local[4]"))
  }

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger.setLevel(Level.OFF)
  }

  def prepareData(sc: SparkContext): (RDD[LabeledPoint], RDD[LabeledPoint], RDD[LabeledPoint], Map[String, Int]) = {
    val rawDataWithHeader = sc.textFile("file:")
    val rawData = rawDataWithHeader.mapPartitionsWithIndex {
      (idx, iter) => if (idx == 0) iter.drop(1) else iter
    }
    val lines = rawData.map(_.split("\t"))
    println("TOTAL RECORDS: " + lines.count())
    val categoriesMap = lines.map(fields => fields(3)).distinct().collect().zipWithIndex.toMap
    val labelpointRDD = lines.map { fields => {
      val trFields = fields.map(_.replace("\"", ""))
      val categoryFeatureArray = Array.ofDim[Double](categoriesMap.size)
      val categoryIdx = categoriesMap(fields(3))
      categoryFeatureArray(categoryIdx) = 1
      val numericalFeatures = trFields.slice(4, fields.length - 1).map(d => if (d == "?" || d.toDouble < 0.0) 0.0 else d.toDouble)
      val label = trFields(fields.length - 1).toInt
      LabeledPoint(label, Vectors.dense(categoryFeatureArray ++ numericalFeatures))
    }}
    val featureData = labelpointRDD.map(labelpoint => labelpoint.features)
    val stdScaler = new StandardScaler(withMean = true, withStd = true).fit(featureData)
    val scaledRDD = labelpointRDD.map(labelpoint => LabeledPoint(labelpoint.label, stdScaler.transform(labelpoint.features)))
    val Array(trainData, validationData, testData)  = scaledRDD.randomSplit(Array(0.8, 0.1, 0.1))
    (trainData, validationData, testData, categoriesMap)
  }

  def trainEvaluate(trainData: RDD[LabeledPoint], validationData: RDD[LabeledPoint]): SVMModel = {
    println("========== START TRAINING MODEL ==========")
    val (model, time) = trainModel(trainData, 0.1)
    val auc = evaluateModel(model, validationData)
    model
  }

  def trainModel(trainData: RDD[LabeledPoint], lambda: Double): (NaiveBayesModel, Double) = {
    val startTime = new DateTime()
    val model = NaiveBayes.train(trainData, lambda)
    val endTime = new DateTime()
    val duration = new Duration(startTime, endTime)
    (model, duration.getMillis)
  }

  def evaluateModel(model: NaiveBayesModel, validationData: RDD[LabeledPoint]): Double = {
    val scoreAndLabel = validationData.map {
      data => {
        var predict = model.predict(data.features)
        (predict, data.label)
      }
    }
    val metrics = new BinaryClassificationMetrics(scoreAndLabel)
    metrics.areaUnderROC()
  }

  def predictData(testData: RDD[LabeledPoint], model: NaiveBayesModel, categoriesMap: Map[String, Int]) = {

  }

}
