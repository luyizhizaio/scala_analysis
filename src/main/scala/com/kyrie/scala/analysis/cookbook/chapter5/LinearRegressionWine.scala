package com.kyrie.scala.analysis.cookbook.chapter5

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.feature.StandardScaler

/**
 * Created by dayue on 2017/5/29.
 */
object LinearRegressionWine extends App {

  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

  val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")

  val sc = new SparkContext(conf)

  val rdd = sc.textFile("data/winequality-red.csv").map(line=>{
    line.split(";")
  })

  val dataPoints = rdd.map(row =>
    new LabeledPoint(row.last.toDouble,Vectors.dense(row.take(row.length-1).map(str =>str.toDouble))))

  //训练集和测试集
  val Array(trainingSet, testSet) = dataPoints.randomSplit(Array(0.8,0.2))

  //特征缩放

  //特征向量
  val featureVector = rdd.map(row =>Vectors.dense(row.take(row.length -1).map(str =>str.toDouble)))
  //统计特征向量，查看特征不是在同一个范围
  val stats = Statistics.colStats(featureVector)
  println(s"Max:${stats.max},min: ${stats.min}, Mean:${stats.mean},Variance:${stats.variance}")
  //标准化，把均值变成0。

  val scaler = new StandardScaler(withMean=true,withStd=true).fit(trainingSet.map(dp=>dp.features))
  val scaledTrainingSet = trainingSet.map(dp => new LabeledPoint(dp.label,scaler.transform(dp.features))).cache

  val scaledTestSet = testSet.map(dp=> new LabeledPoint(dp.label,scaler.transform(dp.features))).cache

  //训练模型
  /*val regression = new  LinearRegressionWithSGD().setIntercept(true)

  regression.optimizer.setNumIterations(1000).setStepSize(0.1)
  val model = regression.run(scaledTrainingSet)


  val predictions:RDD[Double] = model.predict(scaledTestSet.map(point => point.features))

  //评估模型

  val actuals:RDD[Double] = scaledTestSet.map(_.label)

  //计算均方差
  val predictsAndActuals:RDD[(Double,Double)] = predictions.zip(actuals)*/

  val iterations = 1000
  val stepSize =1


  //Create models
  val linearRegWithoutRegularization=algorithm("linear", iterations, stepSize)
  val linRegressionPredictActuals = runRegression(linearRegWithoutRegularization)

  val lasso=algorithm("lasso", iterations, stepSize)
  val lassoPredictActuals = runRegression(lasso)

  val ridge=algorithm("ridge", iterations, stepSize)
  val ridgePredictActuals = runRegression(ridge)

  //Calculate evaluation metrics
  calculateMetrics(linRegressionPredictActuals, "Linear Regression with SGD")
  calculateMetrics(lassoPredictActuals, "Lasso Regression with SGD")
  calculateMetrics(ridgePredictActuals, "Ridge Regression with SGD")



  //正则化参数 ，防止overfiting

  def algorithm(algo:String, iterations:Int,stepSize:Double)=algo match{
    case "linear" =>{
      val algo =new LinearRegressionWithSGD
      algo.setIntercept(true).optimizer.setNumIterations(iterations).setStepSize(stepSize)
      algo
    }
    case "lasso" =>{
      val algo = new LassoWithSGD()
      algo.setIntercept(true).optimizer.setNumIterations(iterations).setStepSize(stepSize).setRegParam(0.001).setMiniBatchFraction(0.05)
      algo
    }
    case "ridge" =>{
      val algo = new RidgeRegressionWithSGD()
      algo.setIntercept(true).optimizer.setNumIterations(iterations).setStepSize(stepSize).setRegParam(0.001).setMiniBatchFraction(0.05)
      algo
    }


  }

  /**
   * 运行回归
   * @param algorithm
   * @return
   */
  def runRegression(algorithm: GeneralizedLinearAlgorithm[_ <: GeneralizedLinearModel]):RDD[(Double,Double)] = {
    val model = algorithm.run(scaledTrainingSet) //Let's pass in the training split

    val predictions: RDD[Double] = model.predict(scaledTestSet.map(point => point.features))
    val actuals: RDD[Double] = scaledTestSet.map(point => point.label)

    //Let's go ahead and calculate the Residual Sum of squares
    val predictsAndActuals: RDD[(Double, Double)] = predictions.zip(actuals)
    predictsAndActuals
  }

  def calculateMetrics(predictsAndActuals:RDD[(Double,Double)], algorithm: String): Unit = {

    val sumSquareErrors = predictsAndActuals.map{case(pred,act) =>
//      println(s"act,pred and differencd $act,$pred , ${act-pred}")
      math.pow(act-pred,2)
    }.sum()

    val meanSquaredError = sumSquareErrors/scaledTestSet.count()

    val meanPrice = dataPoints.map(point => point.label).mean()

    val totalSumOfSquares = predictsAndActuals.map {
      case (pred, act) =>
        math.pow(act - meanPrice, 2)
    }.sum()


    println(s"printing metrics for $algorithm ")

    println(s"SSE IS $sumSquareErrors")

    println(s"MSE is $meanSquaredError ")
    println(s"SST is $totalSumOfSquares")



    val rss= 1 -sumSquareErrors /totalSumOfSquares
    println(s"Residual sum of squares is $rss")

    println(s"************** ending metrics for $algorithm *****************")


  }
  




}
