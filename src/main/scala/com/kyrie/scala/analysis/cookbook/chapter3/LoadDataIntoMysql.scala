//package com.kyrie.scala.analysis.cookbook.chapter3
//
//import java.sql.DriverManager
//
//import com.typesafe.config.ConfigFactory
//import org.apache.spark.sql.SQLContext
//import org.apache.spark.{SparkConf, SparkContext}
//
///**
// * Created by dayue on 2017/5/29.
// */
//object LoadDataIntoMysql extends App {
//
//  val conf = new SparkConf().setAppName("LoadDataIntoMySQL").setMaster("local[2]")
//  val config=ConfigFactory.load() //加载配置文件
//  val sc = new SparkContext(conf)
//  val sqlContext = new SQLContext(sc)
//
//  import  sqlContext.implicits._
//
////  val students = sqlContext.csvFile(filePath = "StudentData.csv", useHeader = true, delimiter = '|')
///*
//
//  +----------+------+-------+-----+------+
//  |   logdate|    pv|reguser|   ip|jumper|
//  +----------+------+-------+-----+------+
//  |2013_05_30|272440|     28|10450|  3717|
//    +----------+------+-------+-----+------+
//
//*/
//  val hmbbs = sc.textFile("data/hmbbs.csv").map{line=>
//    val datas =line.split("\\|")
//    (datas(0),datas(1).toInt,datas(2).toInt,datas(3).toInt,datas(4).toInt)
//  }.toDF
//
//  hmbbs.show()
//
//  hmbbs.foreachPartition{iter=>
//    val conn = DriverManager.getConnection(config.getString("mysql.connection.url"))
//    val statement = conn.prepareStatement("insert into hmbbs.hmbbs_logs_stat (logdate, pv, reguser, ip,jumper) values (?,?,?,?,?) ")
//
//    for(row <-iter){
//      statement.setString(1,row.getString(0))
//      statement.setInt(2,row.getInt(1))
//      statement.setInt(3,row.getInt(2))
//      statement.setInt(4,row.getInt(3))
//      statement.setInt(5,row.getInt(4))
//      statement.addBatch()
//    }
//
//    statement.executeBatch()
//    conn.close()
//
//  }
//
//}
//
