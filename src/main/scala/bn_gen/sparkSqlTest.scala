package bn_gen

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object sparkSqlTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark Read Hive").getOrCreate()
    val sc = spark.sparkContext

    val schema = new StructType {
      Seq(
        StructField("name", StringType, true),
        StructField("age", IntegerType, true)
      )
    }

    /* Some simple examples follow. */
    // read csv file
    val dataCsv1 = spark.read.option("header","true").csv("path")
    val dataCsv2 = spark.read.format("csv").options(Map("header" -> "true"
      ,"sep" -> ",")).load("path")

    // read mysql file
    val dataMysql = spark.read.format("jdbc").options(Map("url" -> "localhost"
      ,"driver" -> "com.mysql.Driver"
      ,"dbtable" -> "test"
      ,"user" -> "root"
      ,"password" -> "123456")).load()

    // read json file
    val dataJson2 = spark.read.format("json").load("path")
    val dataJson2 = spark.read.json("path")

    // create temp view
    dataMysql.createGlobalTempView("name1")
    dataMysql.createOrReplaceTempView("name2")
    dataMysql.createTempView("name3")

    // query data
    val sqlData = spark.sql("select * from name1/name2/name3")
    sqlData.show()

    // save file
    dataCsv1.write.csv("path")
    dataCsv1.write.format("csv").options(Map("header" -> "true"
      ,"sep" -> ",")).save("path")
  }
}
