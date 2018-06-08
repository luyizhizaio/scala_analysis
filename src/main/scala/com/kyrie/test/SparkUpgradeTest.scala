package com.kyrie.test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by tend on 2018/6/6.
 */
object SparkUpgradeTest {


  def main(args: Array[String]) {


    System.setProperty("hadoop.home.dir","C://hadoop" )

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
    val sc = new SparkContext(conf)
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._


    sc.setLogLevel("ERROR")


    val df = sqlContext.read.parquet(s"data/parquet/partiton")

    df.printSchema()

    /*
    root
  |-- t1: string (nullable = true)
  |-- t2: string (nullable = true)
  |-- age: integer (nullable = true)
     */


    val df1 = df.map{row =>
      val t1 = row.getString(0)
      val t2 = row.getString(1)

      t1 -> t2
    }

    df1.printSchema()

    df1.rdd.reduceByKey(_ +"|"+ _).take(10).foreach(println)

//    df.show(10,false)

//    saveToParquet()
  }




  def  saveToParquet()(implicit sqlContext:SQLContext){
    val sc = sqlContext.sparkContext
    import sqlContext.implicits._

    val aa = sc.textFile("data/0.edges")

    val df = aa.map{line =>
      val arr = line.split(" ")
      arr(0) -> arr(1)
    }.toDF("t1","t2")


    df.write.parquet("data/parquet/0edges")

    //      df.show(10,false)

    //    aa.take(10).foreach(println)


  }


  def  saveToText(sqlContext:SQLContext){
    val sc = sqlContext.sparkContext
    import sqlContext.implicits._

    val aa = sc.textFile("data/0.edges")

    val df = aa.map{line =>
      val arr = line.split(" ")
      arr(0) -> arr(1)
    }.toDF("t1","t2")


    df.select("t1").write.text("data/text/0edges")

    //      df.show(10,false)

    //    aa.take(10).foreach(println)


  }

}
