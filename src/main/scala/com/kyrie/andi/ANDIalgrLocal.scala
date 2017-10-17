package com.kyrie.andi

import org.apache.spark.mllib.linalg.{DenseVector, Vectors, Matrix, Matrices}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.linalg.Vector

/**
 * Created by tend on 2017/10/17.
 */
object ANDIalgrLocal {

  def main(args: Array[String]) {


//    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")

//    val sc = new SparkContext(conf)


    //列向量
    var dv:Vector = Vectors.sparse(3, Array(0), Array(2.0))

    val dm:Matrix = Matrices.dense(3, 3, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0,1.0, 8.0, 3.0))
    /*
    1 2 1
    3 4 8
    5 6 3
     */


    var t = 0
    while(t <20){

      dv = dm.multiply(dv)

      println(dv.toString)
      t=t+1

    }

  }

}
