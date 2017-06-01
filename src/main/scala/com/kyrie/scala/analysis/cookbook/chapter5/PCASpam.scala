package com.kyrie.scala.analysis.cookbook.chapter5

import epic.preprocess.{MLSentenceSegmenter, TreebankTokenizer}
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.{MulticlassMetrics, BinaryClassificationMetrics}
import org.apache.spark.mllib.feature.{StandardScaler, IDF, IDFModel, HashingTF}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.regression.{GeneralizedLinearModel, GeneralizedLinearAlgorithm, LabeledPoint}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.mllib.linalg.{Matrix, Vectors, Vector}

/**
 * Created by tend on 2017/6/1.
 */
object PCASpam extends App {



  val conf = new SparkConf().setAppName("PCASpam").setMaster("local[2]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  case class Document(label: String, content: String)

  val docs = sc.textFile("data/SMSSpamCollection").map(line=>{

    val words = line.split("\t")
    Document(words.head.trim,words.tail.mkString(" "))

  }).cache()

  val labeledPointsWithTf = getLabeledPoints(docs)
  val lpTfIdf = withIdf(labeledPointsWithTf).cache()


  //Split dataset
  val spamPoints = lpTfIdf.filter(point => point.label == 1).randomSplit(Array(0.8, 0.2))
  val hamPoints = lpTfIdf.filter(point => point.label == 0).randomSplit(Array(0.8, 0.2))

  println("Spam count:" + (spamPoints(0).count) + "::" + (spamPoints(1).count))
  println("Ham count:" + (hamPoints(0).count) + "::" + (hamPoints(1).count))

  val trainingSpamSplit = spamPoints(0)
  val testSpamSplit = spamPoints(1)

  val trainingHamSplit = hamPoints(0)
  val testHamSplit = hamPoints(1)

  val trainingData = trainingSpamSplit ++ trainingHamSplit
  val testSplit = testSpamSplit ++ testHamSplit

  println ("Training split count : "+trainingData.count())
  println ("Test split count : "+testSplit.count())


  val unlabeledTrainData = trainingData.map(lpoint=> Vectors.dense(lpoint.features.toArray)).cache()

  val scaler = new StandardScaler(withMean = true,withStd=false).fit(unlabeledTrainData)

  val scaledTrainingData =scaler.transform(unlabeledTrainData).cache()

  val trainMaxtrix = new RowMatrix(scaledTrainingData)

  val pcomp :Matrix = trainMaxtrix.computePrincipalComponents(100) //计算主成分

  println("trainMatrix dimension "+ trainMaxtrix.numRows() +"::"+ trainMaxtrix.numCols())
  println("Pcomp dimension  "+pcomp.numRows +"::"+pcomp.numCols)

  //得到降维后的数据
  val reducedTrainingData = trainMaxtrix.multiply(pcomp).rows.cache()
  val reducedTrainingSplit = trainingData.zip(reducedTrainingData).map{case (labeled ,reduced)=>
    new LabeledPoint(labeled.label,reduced)
  }

  //处理训练数据集

  val unlabeledTestData = testSplit.map(lpoint => lpoint.features)
  val testMatrix = new RowMatrix(unlabeledTestData)

  val reducedTestData = testMatrix.multiply(pcomp).rows.cache()
  val  reducedTestSplit = testSpamSplit.zip(reducedTestData).map{case (labeled,reduced)=>new LabeledPoint(labeled.label,reduced)}


  val logisticWithBFGS = getAlgorithm(5, 1, 0.001) //获取算法
  val logisticWithBFGSPredictsActuals = runClassification(logisticWithBFGS, reducedTrainingSplit, reducedTestSplit) //得到真实和预测结果
  calculateMetrics(logisticWithBFGSPredictsActuals, "Logistic with BFGS") //度量






  def getAlgorithm(iterations: Int, stepSize: Double, regParam: Double) = {
    val algo = new LogisticRegressionWithLBFGS()
    algo.setIntercept(true).optimizer.setNumIterations(iterations).setRegParam(regParam)
    algo
  }

  def runClassification(algorithm: GeneralizedLinearAlgorithm[_ <: GeneralizedLinearModel], trainingData: RDD[LabeledPoint],
                        testData: RDD[LabeledPoint]): RDD[(Double, Double)] = {
    val model = algorithm.run(trainingData)
    println ("predicting")
    val predicted = model.predict(testData.map(point => point.features))
    val actuals = testData.map(point => point.label)
    val predictsAndActuals: RDD[(Double, Double)] = predicted.zip(actuals)
    println (predictsAndActuals.collect)
    predictsAndActuals
  }

  def calculateMetrics(predictsAndActuals: RDD[(Double, Double)], algorithm: String) {

    val accuracy = 1.0 * predictsAndActuals.filter(predActs => predActs._1 == predActs._2).count() / predictsAndActuals.count()
    val binMetrics = new BinaryClassificationMetrics(predictsAndActuals)
    println(s"************** Printing metrics for $algorithm ***************")
    println(s"Area under ROC ${binMetrics.areaUnderROC}")
    println(s"Accuracy $accuracy")

    val metrics = new MulticlassMetrics(predictsAndActuals)
    println(s"Precision : ${metrics.precision}")
    println(s"Confusion Matrix \n${metrics.confusionMatrix}")
    println(s"************** ending metrics for $algorithm *****************")
  }






  def getLabeledPoints(docs:RDD[Document]):RDD[LabeledPoint]={
    //Use Scala NLP - Epic
    val labeledPointsUsingEpicRdd: RDD[LabeledPoint] = docs.mapPartitions { docIter =>

      val segmenter = MLSentenceSegmenter.bundled().get
      val tokenizer = new TreebankTokenizer()
      val hashingTf = new HashingTF(5000)

      docIter.map { doc =>
        val sentences = segmenter.apply(doc.content)
        val features = sentences.flatMap(sentence => tokenizer(sentence))

        //consider only features that are letters or digits and cut off all words that are less than 2 characters
        features.toList.filter(token => token.forall(_.isLetterOrDigit)).filter(_.length() > 1)

        new LabeledPoint(if (doc.label.equals("ham")) 0 else 1, hashingTf.transform(features))
      }
    }.cache()

    labeledPointsUsingEpicRdd


  }

  def withIdf(lPoints: RDD[LabeledPoint]): RDD[LabeledPoint] = {
    val hashedFeatures = labeledPointsWithTf.map(lp => lp.features)
    val idf: IDF = new IDF()
    val idfModel: IDFModel = idf.fit(hashedFeatures)

    val tfIdf: RDD[Vector] = idfModel.transform(hashedFeatures)

    val lpTfIdf = labeledPointsWithTf.zip(tfIdf).map {
      case (originalLPoint, tfIdfVector) => {
        new LabeledPoint(originalLPoint.label, tfIdfVector)
      }
    }

    lpTfIdf
  }
}
