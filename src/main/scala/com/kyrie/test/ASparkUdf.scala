package com.kyrie.test

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by tend on 2017/5/26.
 */
object ASparkUdf {

  def main(args: Array[String]) {


    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val dataset = Seq((0, "hello"), (1, "world")).toDF("id", "text")

    val upper:String => String =_.toUpperCase()





    import org.apache.spark.sql.functions.udf
    val upperUDF = udf(upper)

    dataset.withColumn("upper",upperUDF($"text")).show()

//    +---+-----+-----+
//    | id| text|upper|
//    +---+-----+-----+
//    |  0|hello|HELLO|
//    |  1|world|WORLD|
//    +---+-----+-----+



  }

}
