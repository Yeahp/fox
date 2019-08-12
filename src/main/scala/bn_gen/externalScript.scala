package bn_gen

import org.apache.spark.{SparkConf, SparkContext, SparkFiles}

object externalScript {
  /* Here, we use a script written in R as an external file.
  #!/usr/bin/env Rscript
  f <- file("stdin")
  open(f)
  while(length(line <- readlines(f, n=1)) > 0) {
    content <- Map(as.numeric, line.strsplit(",")
    dist <- content[[1]][1] + content[[1]][2]
  }
  write(dist, stdout())
   */
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Spark With External Script")
    val sc = new SparkContext(sparkConf)

    val scriptDir = "..."
    val scriptName = "..."
    sc.addFile(scriptDir)
    val result = sc.textFile("path")
      .map( x => x.map( y => s"$y.col1,$y.col2,$y.col3,$y.col4"))
      // org.apache.spark.rdd.RDD[scala.collection.immutable.IndexedSeq[String]] = MapPartitionsRDD
      .pipe(Seq(SparkFiles.get(scriptName)))
  }
}