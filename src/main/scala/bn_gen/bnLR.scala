package bn_gen

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vector, Vectors}
import org.apache.spark.mllib.optimization.{LBFGS, LogisticGradient, SquaredL2Updater}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.GenIterable
import scala.collection.mutable.ArrayBuffer

object bnLR {
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
    var outputmodel = ""
    var highvarfeaidx = ""
    var corrmatrix = ""
    var clusteridx = ""

    args.sliding(2,2).toList.collect {
      case Array("-inputfile", argInputFile: String) => inputfile = argInputFile
      case Array("-outputmodel", argOutputModel: String) => outputmodel = argOutputModel
      case Array("-highvarfeaidx", argHighVarFeaIdx: String) => highvarfeaidx = argHighVarFeaIdx
      case Array("-corrmatrix", argCorrMatrix: String) => corrmatrix = argCorrMatrix
      case Array("-clusteridx", argClusterIdx: String) => clusteridx = argClusterIdx
    }

    if(inputfile.length == 0 || outputmodel.length == 0 || highvarfeaidx.length == 0 || corrmatrix.length == 0 || clusteridx.length == 0){
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
      , "is_good_bn"
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
    var target = Seq("is_good_bn")

    val data = spark.read.format("csv").option("sep","\001").load(inputfile).toDF(columns:_*)
    val tagData = data.select(tag.map(c => col(c)):_*)
    val doData = data.select(features.map(c => col(c)):_*).rdd.map(row => {
      val len = row.length
      val arr = new Array[Double](len)
      for(i <- 0 until len){arr(i) = row(i).toString.toDouble}
      arr
    }).persist()
    val targetData = data.select(target.map(c => col(c)):_*)

    ///////////////////////////////////////////////////////////////////////////////////
    //                                                                               //
    //                             FEATURE ENGINEERING                               //
    //                                                                               //
    ///////////////////////////////////////////////////////////////////////////////////

    // STEP 1 -- remove features with small variance
    val varThres: Double = 10.0
    val vctData = doData.map(arr => Vectors.dense(arr))
    val variance = Statistics.colStats(vctData).variance

    // define an ArrayBuffer to store the index of features with large variance
    val highVarFeaIdx = new ArrayBuffer[Int]()
    for(i <- 0 until variance.size){
      if(variance(i) > varThres){highVarFeaIdx += i}
    }

    val subDoData = doData.map(s => {
      val highVarFeaArr = new ArrayBuffer[Double]()
      for(k <- highVarFeaIdx.toArray){highVarFeaArr += s(k)}
      highVarFeaArr.toArray
    })

    /* ********************************************************************************** */
    /* store highVarFeaIdx, the index of features with high variance, for prediction use  */
    /* ********************************************************************************** */
    sc.parallelize(highVarFeaIdx.toList).repartition(1).saveAsTextFile(highvarfeaidx)

    // STEP 2 -- perform logarithm transformation
    val logSubDoData = subDoData.map(s => s.map(k => Math.log(k + 1))).persist()

    // STEP 3 -- correlation analysis
    // define a function to transform a matrix to a RDD[Vector]
    def toRDD(sc: SparkContext, m: Matrix): RDD[Vector] = {
      val columns = m.toArray.grouped(m.numRows)
      val rows = columns.toSeq.transpose
      val vectors = rows.map(row => Vectors.dense(row.toArray))
      sc.parallelize(vectors)
    }

    // STEP 3.1 -- calculate correlation coefficient
    val vctLogSubDoData = logSubDoData.map(s => Vectors.dense(s))
    val corrMatridx = Statistics.corr(vctLogSubDoData, "spearman")
    val numRow = corrMatridx.numRows

    /* ********************************************************************************** */
    /* store corrMatrix, the correlation matrix of features, for prediction use           */
    /* ********************************************************************************** */
    sc.parallelize(List(numRow):::corrMatridx.toArray.toList).repartition(1).saveAsTextFile(corrmatrix)

    // STEP 3.2 -- handle distinct features from correlated feature pairs
    val corr: Double = 0.85   // set the threshold of correlation 0.85
    val corrFeaIdx = new ArrayBuffer[Int]()
    for(i <- 0 until numRow){
      for(j <- i+1 until numRow){
        if(corrMatridx(i,j) >= corr){
          corrFeaIdx += i
          corrFeaIdx += j
        }
      }
    }
    val uniqCorrIdx = corrFeaIdx.toArray.distinct

    // STEP 3.2.1 -- extract distinct features
    val uniqCorrFeaArr = logSubDoData.map(s => {
      val uniqCorrFea = new ArrayBuffer[Double]()
      for(k <- uniqCorrIdx){uniqCorrFea += s(k)}
      uniqCorrFea.toArray
    }).cache()

    // STEP 3.2.2 -- cluster distinct features by K-Means algorithm
    val numUniqCorrFea = uniqCorrFeaArr.first().length
    val numInstance = uniqCorrFeaArr.count().toInt
    val trspUniqCorrFeaMat: Matrix = Matrices.dense(numUniqCorrFea, numInstance, uniqCorrFeaArr.flatMap(r => r).collect())

    val trspUniqCorrFeaVct = toRDD(sc, trspUniqCorrFeaMat).cache()

    val numClusters = 3
    val numIterations = 100
    val kMeansModel = KMeans.train(trspUniqCorrFeaVct, numClusters, numIterations)
    val clusterIdx = kMeansModel.predict(trspUniqCorrFeaVct)
    val clusterIdxArr = clusterIdx.collect()

    /* ********************************************************************************** */
    /* store clusterIdxArr, the cluster index of each feature, for prediction use         */
    /* ********************************************************************************** */
    sc.parallelize(clusterIdxArr.toList).repartition(1).saveAsTextFile(clusteridx)

    // STEP 4 -- generate 3 types of features
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

    // STEP 5 -- combine target and features
    val combinedData = targetData.rdd.map(row => Array(row(0).toString.toDouble))
      .zipWithIndex()
      .map(_.swap)
      .join(threeTypeFeaArr.zipWithIndex().map(_.swap))
      .values
      .map{ case(arr1: Array[Double], arr2: Array[Double]) => arr1 ++ arr2 }

    ///////////////////////////////////////////////////////////////////////////////////
    //                                                                               //
    //                     LOGISTIC REGRESSION MODEL TRAINING                        //
    //                                                                               //
    ///////////////////////////////////////////////////////////////////////////////////

    // STEP 1 -- transform data into suitable format
    // CASE 1 -- if data is stored in disk, load data as follows
    // val labeledPointData = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
    // return type: org.apache.spark.rdd.RDD[org.apache.spark.mllib.regression.LabeledPoint]
    // labeledpoint data is stored as 'label index1:value1 index2:value2 ...'

    // CASE 2 -- if data is stored in RDD, transform data as follows
    val labeledPointData = combinedData.map(s => LabeledPoint(s(0), Vectors.dense(s.slice(1, s.length))))

    // take(n) returns n records with the format as 'Array[org.apache.spark.mllib.regression.LabeledPoint]'
    // take(1)(0) returns one record with the format as 'org.apache.spark.mllib.regression.LabeledPoint'
    // take(1)(0).label returns one record's label, while command 'take(1)(0).label' will return one record's features
    // take(1)(0).label returns the number of one record's features
    val numFeatures = labeledPointData.take(1)(0).features.size

    // STEP 2 -- split data into training set and testing set
    val splits = labeledPointData.randomSplit(Array(0.9, 0.1), seed = 11L)  // 90% as training set, 10% as test set
    val training = splits(0).map(x => (x.label, MLUtils.appendBias(x.features))).cache() //append 1 into the training data as intercept
    val test = splits(1)

    // STEP 3 -- build and train model
    // build parameters of LBFGS optimizer
    val numCorrections = 10
    val convergenceTol = 1e-4
    val maxNumIterations = 20
    val regParam = 0.1
    val initialWeightsWithIntercept = Vectors.dense(new Array[Double](numFeatures + 1))
    val (weightsWithIntercept, loss) = LBFGS.runLBFGS(
      training
      , new LogisticGradient()
      , new SquaredL2Updater()
      , numCorrections
      , convergenceTol
      , maxNumIterations
      , regParam
      , initialWeightsWithIntercept
    )

    val model = new LogisticRegressionModel(
      Vectors.dense(weightsWithIntercept.toArray.slice(0, weightsWithIntercept.size - 1))
      , weightsWithIntercept(weightsWithIntercept.size - 1)
    )
    // weightsWithIntercept.toArray.slice(0, weightsWithIntercept.size - 1) is the weight of feature
    // weightsWithIntercept(weightsWithIntercept.size - 1) is the intercept

    ///////////////////////////////////////////////////////////////////////////////////
    //                                                                               //
    //                      LOGISTIC REGRESSION MODEL SAVING                         //
    //                                                                               //
    ///////////////////////////////////////////////////////////////////////////////////
    model.save(sc, outputmodel)

    ///////////////////////////////////////////////////////////////////////////////////
    //                                                                               //
    //                    LOGISTIC REGRESSION MODEL EVALUATING                       //
    //                                                                               //
    ///////////////////////////////////////////////////////////////////////////////////

    // STEP 1 -- make predictions on testing set
    val testPredictionsAndLabels = test.map(point => {
      val prediction = model.predict(point.features)
      (prediction, point.label)
    })

    // STEP 2 -- evaluate the model
    // STEP 2.1 -- loss of each iteration
    println("loss of each iteration in training process")
    loss.foreach(println)

    // STEP 2.2 -- clear the default threshold(0.5) and get raw score for each instance
    model.clearThreshold
    val testScoreAndLabels = test.map(point => {
      val score = model.predict(point.features)
      (score, point.label)
    })

    // STEP 2.3 -- instantiate metrics object
    val metrics = new BinaryClassificationMetrics(testScoreAndLabels)

    // Precision by threshold
    val precision = metrics.precisionByThreshold()
    precision.collect().foreach{case(t: Double, p: Double) => println("Threshold: " + t.toString + "   Precision: " + p.toString)}

    // recall by threshold
    val recall = metrics.recallByThreshold
    recall.collect().foreach{case(t: Double, r: Double) => println("Threshold: " +t.toString + "   Recall: " + r.toString)}

    // F-measure by threshold
    val f1Score = metrics.fMeasureByThreshold
    f1Score.collect().foreach{case(t: Double, f: Double) => println("Threshold: " + t.toString +"    F-score: " + f.toString + "   Beta = 1.0")}

    val beta = 0.5
    val fScore = metrics.fMeasureByThreshold(beta)
    f1Score.collect().foreach{case(t: Double, f: Double) => println("Threshold: " + t.toString +"    F-score: " + f.toString + "   Beta = 0.5")}

    // Area under precision-recall curve, short for AUPRC
    val auPRC = metrics.areaUnderPR()
    println("Area under precision-recall curve = " + auPRC)

    // Area under ROC, short for AUROC or AUC
    val auROC = metrics.areaUnderROC
    println("Area under ROC = " + auROC)

    spark.close()
  }
}