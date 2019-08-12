package bn_gen

import org.apache.spark.SparkConf
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

import scala.collection.GenIterable
import scala.collection.mutable.ArrayBuffer

object bnLRPredict {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .config(new SparkConf())
      .appName("BN Pre-Gen By LR")
      .getOrCreate()
    val sc = spark.sparkContext

    ///////////////////////////////////////////////////////////////////////////////////
    //                                                                               //
    //                            DEFINE AND READ DATA                               //
    //                                                                               //
    ///////////////////////////////////////////////////////////////////////////////////

    // define arguments
    // TYPE 1 -- path of input and output
    var inputfile = ""
    var outputfile = ""
    var highvarfeaidx = ""
    var corrmatrix = ""
    var clusteridx = ""
    var lrmodel = ""

    args.sliding(2,2).toList.collect {
      case Array("-inputfile", argInputfile: String) => inputfile = argInputfile
      case Array("-outputfile", argOutputfile: String) => outputfile = argOutputfile
      case Array("-highvarfeaidx", argHighVarFeaIdx: String) => highvarfeaidx = argHighVarFeaIdx
      case Array("-corrmatrix", argCorrMatrix: String) => corrmatrix = argCorrMatrix
      case Array("-clusteridx", argClusterIdx: String) => clusteridx = argClusterIdx
      case Array("-lrmodel", argOutputModel: String) => lrmodel = argOutputModel
    }

    if(inputfile.length == 0 || outputfile.length == 0 || highvarfeaidx.length ==0 || corrmatrix.length ==0 || clusteridx.length == 0 || lrmodel.length == 0){
      throw new IllegalArgumentException("Missing Required Arguments")
    }

    // define column names and extract features
    var columns = Seq(
      "site_id"
      , "categ_id"
      , "tag_value_name_map"
      , "aspct_event_cnt"
      , "aspct_item_imprsn_cnt"
      , "aspct_guid_cnt"
      , "aspct_sesn_cnt"
      , "query_event_cnt"
      , "query_item_imprsn_cnt"
      , "query_guid_cnt"
      , "query_sesn_cnt"
      , "all_event_cnt"
      , "all_item_imprsn_cnt"
      , "all_guid_cnt"
      , "all_sesn_cnt"
      , "prp_event_cnt"
      , "prp_item_imprsn_cnt"
      , "prp_guid_cnt"
      , "prp_sesn_cnt"
      , "vi_cnt"
    )
    var tag = Seq(
      "site_id"
      , "categ_id"
      , "tag_value_name_map"
    )
    var features = Seq(
      "aspct_event_cnt"
      , "aspct_item_imprsn_cnt"
      , "aspct_guid_cnt"
      , "aspct_sesn_cnt"
      , "query_event_cnt"
      , "query_item_imprsn_cnt"
      , "query_guid_cnt"
      , "query_sesn_cnt"
      , "all_event_cnt"
      , "all_item_imprsn_cnt"
      , "all_guid_cnt"
      , "all_sesn_cnt"
      , "prp_event_cnt"
      , "prp_item_imprsn_cnt"
      , "prp_guid_cnt"
      , "prp_sesn_cnt"
      , "vi_cnt"
    )

    val data = spark.read.format("csv").option("sep","\001").load(inputfile).toDF(columns:_*)
    val tagData = data.select(tag.map(c => col(c)):_*)
    val doData = data.select(features.map(c => col(c)):_*).rdd.map(row => {
      val len = row.length
      val arr = new Array[Double](len)
      for(i <- 0 until len){arr(i) = row(i).toString.toDouble}
      arr
    }).persist()

    ///////////////////////////////////////////////////////////////////////////////////
    //                                                                               //
    //                             FEATURE PROCESSING                                //
    //                                                                               //
    ///////////////////////////////////////////////////////////////////////////////////

    // STEP 1 -- remove features with high variance
    val highVarFeaIdx = sc.textFile(highvarfeaidx).collect().map(_.toInt)

    val subDoData = doData.map(s => {
      val highVarFeaArr = new ArrayBuffer[Double]()
      for(k <- highVarFeaIdx){highVarFeaArr += s(k)}
      highVarFeaArr.toArray
    })

    // STEP 2 -- perform logarithm transformation
    val logSubDoData = subDoData.map(s => s.map(k => Math.log(k + 1))).persist()

    // STEP 2 -- handle highly correlated features
    // STEP 2.1 -- read correlation matrix
    val corrInfo = sc.textFile(corrmatrix).collect()
    val numRow = corrInfo(0).toInt
    val corrMatridx = corrInfo.slice(1,corrInfo.length).map(_.toDouble).sliding(numRow,numRow).toArray

    // STEP 2.2 -- recognize index of high correlated features
    val corr: Double = 0.85   // set the same threshold of correlation 0.85
    val corrFeaIdx = new ArrayBuffer[Int]()
    for(i <- 0 until numRow){
      for(j <- i+1 until numRow){
        if(corrMatridx(i)(j) >= corr){
          corrFeaIdx += i
          corrFeaIdx += j
        }
      }
    }
    val uniqCorrIdx = corrFeaIdx.distinct

    // STEP 2.3 -- read cluster index of distinct correlated features
    val clusterIdxArr = sc.textFile(clusteridx).collect().map(_.toInt)
    val numClusters = clusterIdxArr.distinct.length

    // STEP 3 -- generate three types of features
    val threeTypeFeaArr = logSubDoData.map(s => {
      // TYPE 1 -- standalone features that do not correlate with the others
      val stdAloneFeaArr = new ArrayBuffer[Double]()
      for(k <- 0 until s.length){
        if(!uniqCorrIdx.contains(k)){
          stdAloneFeaArr += s(k)
        }
      }

      // TYPE 2 -- differences feature for difference between correlated features
      val diffFeaArr = new ArrayBuffer[Double]()
      for(pair <- corrFeaIdx.sliding(2,2)){diffFeaArr += s(pair(1)) - s(pair(0))}

      // TYPE 3 -- intergration features that intergrate features from the same cluster
      val interFeaArr_ = new ArrayBuffer[Double]()
      for(k <- uniqCorrIdx){interFeaArr_ += s(k)}
      val clstFeaTup = clusterIdxArr.zip(GenIterable.apply(interFeaArr_.toSeq:_*))
      val interFeaArr = new ArrayBuffer[Double]()
      for(k <- 0 until numClusters){
        var sum = 0.0
        var cnt = 0
        for(item <- clstFeaTup){
          if(item._1 == k){
            cnt += 1
            sum += item._2
          }
        }
        interFeaArr += sum / cnt
      }

      Array(stdAloneFeaArr.toArray, diffFeaArr.toArray, interFeaArr.toArray).flatMap(r => r)
    })

    ///////////////////////////////////////////////////////////////////////////////////
    //                                                                               //
    //                              MAKE PREDICTIONS                                 //
    //                                                                               //
    ///////////////////////////////////////////////////////////////////////////////////

    // STEP 1 -- transform data
    val labeledPointData = threeTypeFeaArr.map(s => LabeledPoint(0, Vectors.dense(s.slice(0,s.length)))).cache()

    // STEP 2 -- read model
    val model = LogisticRegressionModel.load(sc, lrmodel)
    // detailed information includes:
    println(model.intercept)  // the interception
    println(model.weights)  // the weight of each feature
    println(model.numClasses)  // the number of classes
    println(model.numFeatures)  // the number of features
    println(model.getThreshold) // the threshold to determine positive and negative instance

    // STEP 3 --  make predictions
    // STEP 3.1 -- clear the default threshold(0.5)
    model.clearThreshold()

    // STEP 3.2 -- compute raw score for each instance
    val score = labeledPointData.map(point => model.predict(point.features))

    // STEP 3.3 -- combine tag and score
    val combos = tagData.rdd.zipWithIndex()
      .map(_.swap)
      .join(score.zipWithIndex().map(_.swap))
      .values
      .map{case(tg: Row, x: Double) => Row.fromSeq(tg.toSeq :+ x)}

    // STEP 3.4 -- save the result
    val output = spark.createDataFrame(combos, tagData.schema.add("score", DoubleType, false))
    output.write.format("csv").options(Map("sep" -> "\001", "header" -> "true")).save(outputfile)

    spark.close()
  }
}
