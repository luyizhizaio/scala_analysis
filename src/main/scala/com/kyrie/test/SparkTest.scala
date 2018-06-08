package com.kyrie.test

import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by tend on 2018/5/23.
 */
object SparkTest {


  def main(args: Array[String]) {





    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()

    val xx = spark.createDataFrame(Seq(("a", "1|x|s"), ("a", "2"), ("b", "x")))

    xx.foreach(println(_))

  }

}
