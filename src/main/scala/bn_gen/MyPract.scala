package bn_gen

//package com.eric.pract

import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.AccumulatorParam
//import org.apache.spark.util.AccumulatorV2 // spark 2 or above

object MyPract {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("we").setMaster("local")
    val sc = new SparkContext(conf)
    val accum = sc.doubleAccumulator
    val data = sc.makeRDD(List(1.0,2.0,3.0,4.0,5.0))
    println(accum.value)
    val d = data.map(s => {accum.add(s); s})
    d.cache()
    println(accum.value)
  }
}
