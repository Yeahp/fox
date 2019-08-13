package bn

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD

object BNFPGrowth {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("BN Pre-Gen By FP-Growth")
    val sc = new SparkContext(sparkConf)

    var input = ""
    var output = ""
    var minSupp = ""
    var numPart = ""
    var numRePart = ""

    args.sliding(2, 2).collect {
      case Array("-input", argInput: String) => input = argInput
      case Array("-output", argOutput: String) => output = argOutput
      case Array("-minSupp", argMinSupp: String) => minSupp = argMinSupp
      case Array("-numPart", argNumPart: String) => numPart = argNumPart
      case Array("-numRePart", argNumRePart: String) => numRePart = argNumRePart
    }

    if(input.length == 0 || output.length == 0){
      throw new IllegalArgumentException("expected arguments: -input Inputpath -output Outputpath")
    }

    // read data with 23 columns
    val data = sc.textFile(input)  // data: RDD[String]
    val dataArr = data.map(s => s.split("\001"))  // dataArr: RDD[Array[String]]

    // integrate the unique tags for each instance as itemset
    val dataSub = dataArr.map(s => {
      val aspectNameValue = s(2).split('\002')
      var str = ""
      for(item <- aspectNameValue){
        str += s(0) + '\004' + s(1) + '\004' + item + '\002'
      }
      Array(str.substring(0,str.length-1), s(11), s(14))
    })

    // calculate CV for all_event_cnt and all_session_cnt
    val allEventCnt = dataSub.map(s => s(1).toDouble)
    val allSessionCnt = dataSub.map(s => s(2).toDouble)
    val cvAllEventCnt = allEventCnt.stdev / allEventCnt.mean
    val cvAllSessionCnt = allSessionCnt.stdev / allSessionCnt.mean
    val weightAllEventCnt = cvAllEventCnt / (cvAllEventCnt + cvAllSessionCnt)
    val weightAllSessionCnt = cvAllSessionCnt / (cvAllEventCnt + cvAllSessionCnt)

    // duplicate itemset
    val tran = dataSub.map(s => {
      val cnt = (s(1).toDouble*weightAllEventCnt + s(2).toDouble * weightAllSessionCnt).toInt
      val strArr = new Array[String](cnt)
      for(i <- 0 to cnt-1){
        strArr(i) = s(0)
      }
      strArr
    }).flatMap(r => r)

    // construct FP-Growth model and run model
    val transactions: RDD[Array[String]] = tran.map(s => s.split('\002'))
    val fpg = new FPGrowth().setMinSupport(minSupp.toDouble).setNumPartitions(numPart.toInt)
    val model = fpg.run(transactions)

    val formatResult = model.freqItemsets.map(
      itemset => itemset.items.mkString("","\002","")+"\001"+itemset.freq
    ).repartition(numRePart.toInt).saveAsTextFile(output)
  }
}
