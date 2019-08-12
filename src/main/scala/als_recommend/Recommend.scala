package als_recommend

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

object Recommend {

  def main(args: Array[String]): Unit = {
    SetLogger
    println("========== PERIOD-1: PREPARING DATA ==========")

  }

  def recommend(model: MatrixFactorizationModel, movieTitle: Map[Int, String]) = {
    var choose: String = ""
    while (choose != "3") {
      println("please choose recommend type: 1 - recommend movie based on user, 2 - recommend user based on moive, 3 - exit.")
      choose = readLine().toString
      if (choose == "1") {
        println("please enter user ID: ")
        val inputUserID = readLine().toString.toInt
        RecommendMoives(model, movieTitle, inputUserID)
      } else if (choose == "2") {
        print("please enter movie ID: ")
        val inputMovieID = readLine().toString.toInt
        RecommendUsers(model, movieTitle, inputMovieID)
      }
    }
  }

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger.setLevel(Level.OFF)
  }

  def PrepareData(): (RDD[Rating], Map[Int, String]) = {
    val sc = new SparkContext(new SparkConf().setAppName("recommend").setMaster("local[4]"))

    println("start reading data...")
    val rawUserData = sc.textFile("file:")
    val rawRatings = rawUserData.map(_.split("\t").take(3))
    val ratingRDD = rawRatings.map { case Array(user, movie, rating) => Rating(user.toInt, movie.toInt, rating.toDouble)}
    println("totally " + ratingRDD.count().toString + " records!")

    println("start reading movie data...")
    val itemRDD = sc.textFile("file:")
    val movieTitle = itemRDD.map(_.split("|").take(2)).map(array => (array(0).toInt, array(1))).collect().toMap

    val numRatings = ratingRDD.count()
    val numUsers = ratingRDD.map(_.user).distinct().count()
    val numMovies = ratingRDD.map(_.product).distinct().count()
    println("totally: ratings-" + numRatings + ", user-" + numUsers + ", movie" + numMovies)

    (ratingRDD, movieTitle)
  }

  def RecommendMoives(model: MatrixFactorizationModel, movieTitle: Map[Int, String], inputUserID: Int) = {
    val RecommendMovie = model.recommendProducts(inputUserID, 10)
    var i = 1
    println("for user " + inputUserID + ":")
    RecommendMovie.foreach { r =>
      println(i.toString + ": " + movieTitle(r.product) + " rating-" + r.rating)
      i += 1
    }
  }

  def RecommendUsers(model: MatrixFactorizationModel, movieTitle: Map[Int, String], inputMovieID: Int) = {
    val RecommendUsers = model.recommendUsers(inputMovieID, 10)
    var i = 1
    println("for movie " + inputMovieID + ":")
    RecommendUsers.foreach { r =>
      println(i.toString + ": " + movieTitle(r.user) + " rating-" + r.rating)
      i += 1
    }
  }

}
