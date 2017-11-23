package com.kyrie

import _root_.breeze.linalg.{*, DenseVector, DenseMatrix}
import _root_.scala.util.control.Breaks._


/**
 * Created by tend on 2017/11/22.
 * 感知机
 */
object PLA {

  def main(args: Array[String]) {

    //按照行创建矩阵	DenseMatrix((1.0,2.0),(3.0,4.0))
    val dm = DenseMatrix((1.0, 0.10723, 0.64385, 0.29556, 1.0),
      (1.0, 0.2418, 0.83075, 0.42741, 1.0),
      (1.0, 0.23321, 0.81004, 0.98691, 1.0),
      (1.0, 0.36163, 0.14351, 0.3153, -1.0),
      (1.0, 0.46984, 0.32142, 0.00042772, -1.0),
      (1.0, 0.25969, 0.87208, 0.075063, -1.0))

    println(dm)
    //权重向量
    var w = DenseMatrix((1.0, 1.0, 1.0, 1.0))

    var count = 0
    breakable {

      while (true) {
        count += 1
        var iscompleted = true

        for (i <- 0 until dm.rows) {
          val X = dm(i, 0 to -2).t
          val Y = w * X
          breakable {

            if (sign(Y(0)) == sign(dm(i, -1))) {
              //continue
              break
            } else {
              iscompleted = false
              //update weight
              w = w + DenseMatrix(dm(i, -1) * X)
              println(s"w:$w;count:$count")

            }

          }

        }
        if (iscompleted) {
          break
        }

      }
    }
    println(s"final w:$w")
    println(s"count:$count")

  }

  def sign(num:Double)={
    if(num >0.0) 1.0 else if(num == 0.0) 0.0 else -1.0

  }

}
