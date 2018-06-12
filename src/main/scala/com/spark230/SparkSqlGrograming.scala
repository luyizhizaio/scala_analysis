package com.spark230

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StructType, StructField, StringType}

/**
 * Created by tend on 2018/6/8.
 */
object SparkSqlGrograming {


  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","C://hadoop" )

    implicit val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._

    createDs()
    runProgrammaticSchema
  }


  def  createDs()(implicit spark:SparkSession)={
    import spark.implicits._

    val strDS = Seq("aa","bb").toDS()
    strDS.show(10)
  }


  def  runProgrammaticSchema()(implicit spark:SparkSession)={
    import spark.implicits._

    val peopleRDD = spark.sparkContext.textFile("data/people.txt")

    val schemaString = "name age"
    val fields = schemaString.split(" ").map{fieldName => StructField(fieldName,StringType,nullable = true)}

    val schema = StructType(fields)

    val rowRDD = peopleRDD.map(_.split(","))
    .map(attr => Row(attr(0), attr(1).trim))


    val peopleDF = spark.createDataFrame(rowRDD,schema)

    peopleDF.show(10)
  }

}
