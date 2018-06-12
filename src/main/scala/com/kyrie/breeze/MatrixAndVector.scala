package com.kyrie.breeze

import breeze.linalg.DenseMatrix

/**
 * Created by tend on 2017/10/16.
 */
object MatrixAndVector {

  def main(args: Array[String]) {


    val dm = DenseMatrix((1.0, 2.0, 3.0),
      (4.0, 5.0, 6.0))

    println(dm)

    println(dm(1,0 to -2))
    println(dm(1,-1))

  }

}
