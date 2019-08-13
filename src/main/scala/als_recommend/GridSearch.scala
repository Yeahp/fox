package als_recommend

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.jfree.data.category.DefaultCategoryDataset
import org.joda.time.{DateTime, Duration}

object GridSearch {

  def main(args: Array[String]): Unit = {
    val (ratingRDD, movieTitle) = Recommend.PrepareData()
    val Array(trainData: RDD[Rating], validationData: RDD[Rating], testData: RDD[Rating]) = ratingRDD.randomSplit(Array(0.8, 0.1, 0.1))
    trainValidation(trainData, validationData)
  }

  def trainValidation(trainData: RDD[Rating], validationData: RDD[Rating]): MatrixFactorizationModel = {
    println("========== EVALUATE PARAM: RANK ==========")
    evaluateParameter(trainData, validationData, "rank", Array(5, 10, 15, 20, 50, 100), Array(10), Array(0.1))

    println("========== EVALUATE PARAM: NUMBER OF ITERATIONS ==========")
    evaluateParameter(trainData, validationData, "numIterations", Array(10), Array(5, 10, 15, 20, 25), Array(0.1))

    println("========== EVALUATE PARAM: LAMBDA ==========")
    evaluateParameter(trainData, validationData, "rank", Array(10), Array(10), Array(0.05, 0.1, 1, 5, 10))

    println("========== EVALUATE PARAM: PARAM COMBINATIONS ==========")
    evaluateAllParameter(trainData, validationData, "rank", Array(5, 10, 15, 20, 50, 100), Array(5, 10, 15, 20, 25), Array(0.05, 0.1, 1, 5, 10))
  }

  def evaluateParameter(trainData: RDD[Rating], validationData: RDD[Rating], evaluateParameter: String, rankArray: Array[Int], numIterationsArray: Array[Int], lambdaArray: Array[Double]) = {
    var dataBarChart = new DefaultCategoryDataset()
    var dataLineChart = new DefaultCategoryDataset()
    for (rank <- rankArray; numIterations <- numIterationsArray; lambda <- lambdaArray) {
      val (rmse, time) = trainModel(trainData, validationData, rank, numIterations, lambda)
      val parameterData =
        evaluateParameter match {
          case "rank" => rank;
          case "numIterations" => numIterations;
          case "lambda" => lambda
        }
      dataBarChart.addValue(rmse, evaluateParameter, parameterData.toString)
      dataLineChart.addValue(time, "TIME", parameterData.toString)
      Chart.plotBarLineChart("ALS_EVALUATIONS " + evaluateParameter, evaluateParameter, "RMSE", 0.58, 5, "TIME", dataBarChart, dataLineChart)
    }
  }

  def trainModel(trainData: RDD[Rating], validationData: RDD[Rating], rank: Int, numIterations: Int, lambda: Double): (Double, Double) = {
    val startTime = new DateTime()
    val model = ALS.train(trainData, rank, numIterations, lambda)
    val endTime = new DateTime()
    val rmse = computeRmse(model, validationData)
    val duration = new Duration(startTime, endTime)
    println(f"TRAIN PARAMETER: RANK-$rank%3d, ITERATIONS-$numIterations%3d, LAMBDA-$lambda%.2f, RMSE-$rmse%.2f, TIME-" + duration.getMillis + "mills")
    (rmse, duration.getStandardSeconds)
  }

  def computeRmse(model: MatrixFactorizationModel, ratingRDD: RDD[Rating]): Double = {
    val num = ratingRDD.count()
    val predictedRDD = model.predict(ratingRDD.map(r => (r.user, r.product)))
    val predictedAndRatings =
      predictedRDD.map(p => ((p.user, p.product), p.rating))
        .join(ratingRDD.map(r => ((r.user, r.product), r.rating)))
        .values
    math.sqrt(predictedAndRatings.map(x => math.pow(x._1 - x._2, 2)).reduce(_ + _) / num)
  }

  def evaluateAllParameter(trainData: RDD[Rating], validationData: RDD[Rating], evaluateParameter: String, rankArray: Array[Int], numIterationsArray: Array[Int], lambdaArray: Array[Double]): MatrixFactorizationModel = {
    val evaluations =
      for (rank <- rankArray; numIterations <- numIterationsArray; lambda <- lambdaArray) yield {
        val (rmse, time) = trainModel(trainData, validationData, rank, numIterations, lambda)
        (rank, numIterations, lambda, rmse)
      }
    val eval = evaluations.sortBy(_._4)
    val bestEval = eval(0)
    println("BEST PARAMETER: RANK-" + bestEval._1 + ", ITERATIONS- " + bestEval._2 + ", LAMBDA-" + bestEval._3 + ", RMSE-" + bestEval._4)
    ALS.train(trainData, bestEval._1, bestEval._2, bestEval._3)
  }

}
