//package com.kyrie.scala.analysis.cookbook.chapter3
//
//import com.typesafe.config.ConfigFactory
//import org.apache.spark.sql.SQLContext
//import org.apache.spark.{SparkContext, SparkConf}
//
///**
// * Created by dayue on 2017/5/29.
// */
//object LoadDataFromMysql extends App {
//
//  val conf = new SparkConf().setAppName("LoadDataIntoMySQL").setMaster("local[2]")
//  val config=ConfigFactory.load() //加载配置文件
//  val sc = new SparkContext(conf)
//  val sqlContext = new SQLContext(sc)
//
//
//  val  options = Map(
//    "driver"->config.getString("mysql.driver"),
//    "url" -> config.getString("mysql.connection.url"),
//    "dbtable" -> "(select * from hmbbs_logs_stat) as  hmbbs_logs_stat",
//    "partitonColumn" -> "id",
//    "lowerBound"-> "1",
//    "upperBound" -> "100",
//    "numPartitions" -> "2"
//  )
//
//  val df = sqlContext.load("jdbc",options)
//
//  df.printSchema()
//
//  df.show()
//
//
//
//}
