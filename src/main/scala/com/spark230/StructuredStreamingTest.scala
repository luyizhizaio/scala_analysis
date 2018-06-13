package com.spark230

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Created by tend on 2018/6/12.
 */
object StructuredStreamingTest {

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir","C://hadoop" )

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._


    //接收socket请求
    //
    // lines代表包含流数据的无界的表，该表包含列名为“value”的列。
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 19999)
      .load()

    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()


    val query = wordCounts.writeStream
    .outputMode("complete")
    .format("console")
    .start()

    query.awaitTermination()




  }

}
