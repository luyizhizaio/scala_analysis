package com.kyrie.scala.analysis.cookbook.chapter3

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkContext, SparkConf}

import scala.io.Source
import scala.reflect.io.File

/**
 * Created by dayue on 2017/5/29.
 */
object DataFrameFromJSON extends App {
  val conf = new SparkConf().setAppName("DataFromJSON").setMaster("local[2]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  val dFrame=sqlContext.jsonFile("/Users/Gabriel/Dropbox/arun/ScalaDataAnalysis/Code/scaladataanalysisCB-tower/chapter3-data-loading/profiles.json")

  //val dFrame = sqlContext.jsonFile("hdfs://localhost:9000/data/scalada/profiles.json")
  dFrame.printSchema()
  dFrame.show()

  //Using JSONRDD
  //val strRDD = sc.textFile("hdfs://localhost:9000/data/scalada/profiles.json")
  val strRDD = sc.textFile("/Users/Gabriel/Dropbox/arun/ScalaDataAnalysis/Code/scala-dataanalysis-cookbook/chapter3-data-loading/profiles.json")
  val jsonDf = sqlContext.jsonRDD(strRDD)

  jsonDf.printSchema()
  jsonDf.show()

  //Explicit Schema Definition
  val profilesSchema = StructType(
    Seq(
      StructField("_id", StringType, true),
      StructField("about", StringType, true),
      StructField("address", StringType, true),
      StructField("age", IntegerType, true),
      StructField("company", StringType, true),
      StructField("email", StringType, true),
      StructField("eyeColor", StringType, true),
      StructField("favoriteFruit", StringType, true),
      StructField("gender", StringType, true),
      StructField("name", StringType, true),
      StructField("phone", StringType, true),
      StructField("registered", TimestampType, true),
      StructField("tags", ArrayType(StringType), true)))

  val jsonDfWithSchema = sqlContext.jsonRDD(strRDD, profilesSchema)

  jsonDfWithSchema.printSchema() //Has timestamp
  jsonDfWithSchema.show()

  jsonDfWithSchema.registerTempTable("profilesTable")

  //Filter based on timestamp
  val filterCount = sqlContext.sql("select * from profilesTable where registered> CAST('2014-08-26 00:00:00' AS TIMESTAMP)").count

  val fullCount = sqlContext.sql("select * from profilesTable").count

  println("All Records Count : " + fullCount) //200
  println("Filtered based on timestamp count : " + filterCount) //106

  //Writes schema as JSON to file
  File("profileSchema.json").writeAll(profilesSchema.json)

  val loadedSchema = DataType.fromJson(Source.fromFile("profileSchema.json").mkString)

  println ("ProfileSchema == loadedSchema :"+(loadedSchema.json==profilesSchema.json))
  //Print loaded schema
  println(loadedSchema.prettyJson)

}
