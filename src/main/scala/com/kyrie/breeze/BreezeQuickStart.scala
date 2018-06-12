//package com.kyrie.breeze
//
//import breeze.linalg._
//import breeze.numerics.{exp, log}
//import breeze.stats.mean
//import breeze.stats.distributions._
//import breeze.optimize._
///**
// * Created by tend on 2017/10/16.
// */
//object BreezeQuickStart {
//
//  def main(args: Array[String]) {
//
//    val x = DenseVector.zeros[Double](5)
//    //访问元素
//    println(x(0))
//
//    //赋值
//    x(1) = 2
//
//    println(x) //DenseVector(0.0, 2.0, 0.0, 0.0, 0.0)
//
//    //获取第二个元素，倒数第四个元素
//    println(x(-4)) //x(i) == x(x.length + i)
//
//    //给3到4元素赋值
//    x(3 to 4) := .5
//
//    println(x) //DenseVector(0.0, 2.0, 0.0, 0.5, 0.5)
//
//    //指定一个可兼容的向量
//    x(0 to 1) := DenseVector(.1, .2)
//
//
//    println(x) //DenseVector(0.1, 0.2, 0.0, 0.5, 0.5)
//
//
//    //矩阵
//
//    val m = DenseMatrix.zeros[Int](5,5)
//
//    println(m)
//    /*
//    0  0  0  0  0
//    0  0  0  0  0
//    0  0  0  0  0
//    0  0  0  0  0
//    0  0  0  0  0
//*/
//    //矩阵的列作为一个DenseVectors访问，行可以作为一个DenseMatrices
//
//    //行列索引从0开始
//    println((m.rows, m.cols)) //(5,5)
//
//    //获取第二列
//    println(m(::, 1))
//
//
//    //给第5行赋值
//    m(4, ::) := DenseVector(1, 2, 3, 4, 5).t //转置成行向量
//
//    println(m)
//    /*
//    0  0  0  0  0
//    0  0  0  0  0
//    0  0  0  0  0
//    0  0  0  0  0
//    1  2  3  4  5
//*/
//
//    //更新矩阵
//    m(0 to 1, 0 to 1) := DenseMatrix((3, 1), (-1, -2))
//
//    println(m)
//    /*
//    3   1   0  0  0
//    -1  -2  0  0  0
//    0   0   0  0  0
//    0   0   0  0  0
//    1   2   3  4  5
//*/
//
//
//    // Broadcasting
//
//
//    val dm = DenseMatrix((1.0, 2.0, 3.0),
//      (4.0, 5.0, 6.0))
//
//    //每列加一个列向量
//    val res = dm(::, *) + DenseVector(3.0, 4.)
//
//    println(res)
//
//
//    /*
//    4.0  5.0  6.0
//    8.0  9.0  10.0
//*/
//
//    //修改所有的列
//    res(::, *) := DenseVector(3., 4.)
//
//    println(res)
//    /*
//
//    3.0  3.0  3.0
//    4.0  4.0  4.0
//*/
//
//    //计算行的均值
//    println(mean(dm(*, ::)))
//    //DenseVector(2.0, 5.0)
//
//
//    //概率分布
//    println("------probability distributions---------")
//
//    //创建均值为3的泊松分布,
//    val poi = new Poisson(3.0)
//
//    //抽样
//    val s = poi.sample(5)
//    println(s) //Vector(4, 3, 4, 5, 1)
//
//    //返回每个元素的概率
//    println(s.map {
//      poi.probabilityOf(_)
//    })
//    //  Vector(0.22404180765538775, 0.22404180765538775, 0.16803135574154085, 0.10081881344492458, 0.22404180765538775)
//
//
//    val doublePoi = for (x <- poi) yield x.toDouble
//
//
//    //抽样数据的均值和方差
//    println(breeze.stats.meanAndVariance(doublePoi.samples.take(1000)))
//    // MeanAndVariance(3.017000000000002,3.009720720720717,1000)
//
//
//    //均值和方差
//    println((poi.mean, poi.variance))
//    //  (3.0,3.0)
//
//
//    //指数分布
//
//    val expo = new Exponential(0.5)
//
//    println(expo.rate) //0.5
//
//    //指数分布的特点是它的半衰期，我们可以计算一个值在两个数之间的概率。
//
//    println(expo.probability(0,log(2) * expo.rate))
//    //    0.5
//
//
//    println(expo.probability(0.0,1.5))
////    0.950212931632136
//
//
//
//    println(1 - exp(-3.0))
////    0.950212931632136
//
//    val samples = expo.sample(2).sorted
//
//
//    println(expo.probability(samples(0),samples(1)))
//    //    0.22505753016992747
//
//    println(breeze.stats.meanAndVariance(expo.samples.take(10000)))
//    //    MeanAndVariance(1.9914940375051924,3.8625097609464274,10000)
//
//
//    println(1 /expo.rate, 1/ (expo.rate * expo.rate))
////    (2.0,4.0)
//
//
//
//
///*
//
//    val f = new DiffFunction[DenseVector[Double]] {
//
//      override def calculate(x: DenseVector[Double]): (Double, DenseVector[Double]) = {
//
//        (norm((x - 3d) :^ 2d,1d),(x * 2d) - 6d);
//
//      }
//    }
//*/
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//  }
//
//}
