package com.kyrie

import _root_.breeze.linalg.{sum, DenseMatrix}


/**
 * Created by tend on 2017/11/23.
 */
object LinearRegression {

  def main(args: Array[String]) {

    val data = DenseMatrix((1.0,3.0),
      (1.2,3.0),
      (1.2,4.0),
      (1.5,4.5),
      (1.6,4.3),
      (6.5,12.0))

/*
    val N = data.rows.toFloat

    var w = 0.0

    val learning_rate = 0.01
    val iter = 20


    for( i <- 0 to iter) {


      val x = data(::, 0)
      val y = data(::, 1)


      // partial derivative function of w
      var w_gradient = (2 / N) * (x.t * x * w - x.t * y)


      w = w - learning_rate * w_gradient

      println(w)
      println("square error:" + compute_square_error(w, data))

    }*/

    val learning_rate = 0.01
    val iter = 1000
    val w = linearRegression(data,learning_rate,iter)

    println(s"final w:$w")

  }


  /**
   * square error measure
   * @param w
   * @param data
   * @return
   */
  def compute_square_error(w:Double,b:Double,data:DenseMatrix[Double]): Double ={
    val x = data(::,0)
    val y = data(::,1)

    val totalError =  (y - w * x :-=b) :* (y - w * x :-=b)



    sum(totalError) /data.rows
  }


  def linearRegression(data:DenseMatrix[Double],learning_rate:Double,iter:Int): Double ={
    //define hyperparameters
    var w:Double = 0.0
    var b:Double = 0.0
    for( i <- 0 to iter){

      val xx = computeGradient(data,w,b,learning_rate)
      w = xx._1
      b =xx._2
      if(i % 10 ==0){
        println(s"iter $i:error=${compute_square_error(w ,b,data)}")
      }

    }
    w
  }

  def computeGradient(data:DenseMatrix[Double],w_current:Double,b_current:Double,learning_rate:Double)={

    val N = data.rows.toDouble

    val x =data(::,0)
    val y =data(::,1)

    var b_gradient = (2/N) * (y - x :*= w_current :-= b_current)

    // partial derivative function of w
    var w_gradient = (2/N) * (x.t * (  x :*= w_current :-= y :+= b_current))

    //substract
    val b= b_current - learning_rate * sum(b_gradient)
    val w = w_current - learning_rate * w_gradient

    (w,b)
  }

}
