package com.kyrie.andi

import org.apache.spark.mllib.linalg.Vectors

/**
 * Created by tend on 2017/10/11.
 */
object ATest {


  def main(args: Array[String]) {


    val r0 = Vectors.zeros(98)

    val arr = r0.toArray

      arr.foreach(println(_))

    println("length:"+arr.length)

  }

}
