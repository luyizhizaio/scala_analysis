package com.kyrie.scala.analysis.cookbook.chapter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by dayue on 2017/5/26.
 */
object Chapter3 {

  def main(args: Array[String]) {
    import org.apache.spark.sql

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val rddOfStudents = convertToStudents("data/student-mat.csv",sc)

    import sqlContext.implicits._

    val studentDF = rddOfStudents.toDF()
    studentDF.printSchema()

    studentDF.show()


  }

  //转换成Student对象
  def convertToStudents(filePath:String,sc:SparkContext):RDD[Student]={

    val rddOfStudents:RDD[Student] = sc.textFile(filePath).flatMap(eachLine => Student(eachLine))

    rddOfStudents
  }


}
