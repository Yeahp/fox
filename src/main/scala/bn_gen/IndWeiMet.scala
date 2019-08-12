package bn_gen

import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{DenseMatrix, Matrices, Vector, Vectors}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{lit, _}
import org.apache.spark.sql.types.{DoubleType, StructType}
import org.apache.spark.{SparkConf, sql}

import scala.collection.mutable._

object IndWeiMet {
  def main(args: Array[String]): Unit = {
    var input = ""
    var output = ""

    args.sliding(2, 2).toList.collect {
      case Array("-input", argInput: String) => input = argInput
      case Array("-output", argOutput: String) => output = argOutput
    }

    println("input:" + input)
    println("output:" + output)

    if(input.length == 0 || output.length == 0){
      throw new IllegalArgumentException("expected arguments: -input GBLFileInputpath -output CSVOutputpath")
    }

    //val input = "C:/Users/aisun/Desktop/Projects/2017Q1/GBL_ranking/Prototype/input_file.dat"
    //val output = "C:/Users/aisun/Desktop/Projects/2017Q1/GBL_ranking/Prototype/output.dat"
    //val spark  = SparkSession.builder().master("local").appName("GBL ranking").getOrCreate()

    val spark  = SparkSession.builder().config(new SparkConf()).getOrCreate()
    val sc = spark.sparkContext

    val inputCols= Seq(
      "SITE_ID"
      ,"CATEG_ID"
      , "ASPCT_CNCL_VALUE"
      ,"DF_NF_ITEM_CNT"
      ,"DF_CLSFR_ITEM_CNT"
      ,"UMS_CATEG_CNT"
      ,"QUERY_CNT"
      ,"GMV_USD_AMT"
      ,"SI_CNT","PROD_CVRG"
      ,"PRP_PROD_CNT"
      ,"PROD_LL_CNT"
      ,"PRP_TRFC_CNT"
      ,"BN_CVRG"
      ,"BN_PV_CNT"
    )
    val metricCols = Seq(
      "DF_NF_ITEM_CNT"
      ,"DF_CLSFR_ITEM_CNT"
      ,"UMS_CATEG_CNT"
      ,"QUERY_CNT"
      ,"GMV_USD_AMT"
      ,"SI_CNT"
      ,"PROD_CVRG"
      ,"PRP_PROD_CNT"
      ,"PROD_LL_CNT"
      ,"PRP_TRFC_CNT"
      ,"BN_CVRG"
      ,"BN_PV_CNT"
    )
    val scoreCol = "Score"

    val inDF = spark.read.format("csv")
      .option("sep","\001")
      .load(input).toDF(inputCols: _*)
    //toDF(colnames：String*)将参数中的几个字段返回一个新的dataframe类型
    inDF.cache()

    //cast column as double (csv read as string)
    val inDF3 = inDF.select(metricCols.map(c => col(c).cast("double")): _*)
    //metricCols.map(c => col(c).cast("double")) will return:
    // ret: Seq[org.apache.spark.sql.Column] = List(CAST(DF_NF_ITEM_CNT AS DOUBLE)
    // , CAST(DF_CLSFR_ITEM_CNT AS DOUBLE)
    // , CAST(UMS_CATEG_CNT AS DOUBLE)
    // , ...
    // , CAST(BN_PV_CNT AS DOUBLE))

    //step0 : create schema for each conversion from RDD[Vector] to DataFrame after normalizer
    var schema = new StructType()
    for(ele <- inDF3.columns.toList){
      schema = schema.add(ele,DoubleType,false)
    }

    //step1 : feature normalization
    import org.apache.spark.ml.feature.VectorAssembler
    val scaler = new MinMaxScaler()
      .setMax(100)
      .setMin(0)

    val assembler = new VectorAssembler()
      .setInputCols(metricCols.toArray)
      .setOutputCol("features")
    val inDF3_Vec = assembler.transform(inDF3)
    //inDF3_Vec: org.apache.spark.sql.DataFrame = [DF_NF_ITEM_CNT: double, ... 11 more fields]

    var featureVectors=scaler
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .fit(inDF3_Vec)
      .transform(inDF3_Vec)
      .select("scaledFeatures")
    //featureVectors: org.apache.spark.sql.DataFrame = [scaledFeatures: vector]

    //convert ml.linalg.vector(DataFrame API) to mllib.linalg.Vector (RDD API)
    featureVectors = MLUtils.convertVectorColumnsFromML(featureVectors)
    //featureVectors: org.apache.spark.sql.DataFrame = [scaledFeatures: vector]

    //Convert normalized feature to matrix to calculate final score in the last step
    val NormizeFeatureRDD = featureVectors.rdd.map(row=>row.getAs[Vector](0))
    val GBLMatrix = new RowMatrix(NormizeFeatureRDD)
    val featureRDD = featureVectors.rdd
      .map(row=>row.getAs[Vector](0))
      .map(v =>Row.fromSeq(v.toArray))
    val NormFeatureDF = spark.createDataFrame(featureRDD,schema)
    //cache NormFeatureDF as it will be used many times in the following iteration
    NormFeatureDF.cache()
    //NormFeatureDF.show(20,false)

    //step2 : calculate coefficient for each metric
    //replace each metric with number 1 as this column will be treat as constant coefficient
    import scala.collection.mutable.ListBuffer

    val weightArray= ListBuffer[Double]()
    for(col <- metricCols) {
      //save each column as vector
      //val yVector = Vectors.dense(NormFeatureDF.select(col).collect().map(row=>row.getAs[Double](0)))
      val yAbsRDD = NormFeatureDF.select(col).rdd.map(row=>row.getAs[Double](0))

      //replace each column with constant 1 (it is taken as the constant value in linear regression
      val f = NormFeatureDF.withColumn(col,lit(1.0))
      val f2= f.withColumn(col,f(col).cast(sql.types.DoubleType)) //cast lit(1.0) as double
      val f3=f2.rdd.map({ case row =>
        Vectors.dense(row.toSeq.toArray.map{
          x => x.asInstanceOf[Double]
        })
      })

      val m = new RowMatrix(f3)
      //m.rows.collect().foreach(println)
      val kvalue = metricCols.length
      val rowCount = m.numRows().toInt

      //use SVD to compute the general inverse of any matrix,
      //but we did not do any elimination for K value
      //as the data volume is relatively small (100K)
      val svd = m.computeSVD(kvalue, true)
      val v = svd.V
      val sInvArray = svd.s.toArray.toList.map(x => 1.0 / x).toArray
      val sInverse = new DenseMatrix(kvalue, kvalue, Matrices.diag(Vectors.dense(sInvArray)).toArray)
      val uArray = svd.U.rows.collect.toList.map(_.toArray.toList).flatten.toArray
      val uTranspose = new DenseMatrix(kvalue, rowCount, uArray) // already transposed because DenseMatrix is column-major
      val inverse = v.multiply(sInverse).multiply(uTranspose)
      val coefficient = inverse.multiply(Vectors.dense(yAbsRDD.collect()))
      val coefficientMatrix = Matrices.dense(1,coefficient.size,coefficient.toArray).transpose

      val yEst =m.multiply(coefficientMatrix)
      //RDD[Vector] ->RDD[Row]->RDD[Double](single element)
      val yEstRDD=yEst.rows.map(v =>Row.fromSeq(v.toArray).getAs[Double](0))
      //weight is the reciprocal of correlation, i.e. the bigger the correlation value,
      //the smaller the weight of this metric
      val correlation = Statistics.corr(yAbsRDD,yEstRDD)
      weightArray.+=(1/correlation)
    }

    val totalWeight=weightArray.toArray.sum
    val IndWeight = weightArray.map(v=>v/totalWeight)

    //step3: calcuate score for each row
    val ScoreMatrix=GBLMatrix.multiply(Matrices.dense(1,IndWeight.size,IndWeight.toArray).transpose)

    //step4: add back to original dataframe
    val ScoreRDD=ScoreMatrix.rows.map(v => Row.fromSeq(v.toArray).getAs[Double](0))
    val rows = inDF.rdd.zipWithIndex.map(_.swap)
      .join(ScoreRDD.zipWithIndex.map(_.swap))
      .values
      .map { case (row: Row, x: Double) => Row.fromSeq(row.toSeq :+ x) }
    // var s = Seq(1.0)
    // s :+ = 2.0
    // res: Seq[Double] = List(1.0, 2.0)

    val finalDF=spark.createDataFrame(rows, inDF.schema.add("Score", DoubleType, false))
    finalDF.write.csv(output)
  }
}
