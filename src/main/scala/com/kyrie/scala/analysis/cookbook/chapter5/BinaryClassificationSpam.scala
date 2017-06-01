package com.kyrie.scala.analysis.cookbook.chapter5

import java.util.Properties
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.io.Source
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation
import edu.stanford.nlp.pipeline.Annotation
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.optimization.Updater
import org.apache.spark.mllib.optimization.L1Updater
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.GeneralizedLinearModel
import org.apache.spark.mllib.regression.GeneralizedLinearAlgorithm
import org.apache.spark.mllib.optimization.SquaredL2Updater
import epic.preprocess.TreebankTokenizer
import epic.preprocess.MLSentenceSegmenter
import org.apache.spark.mllib.feature.IDF
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.feature.Normalizer
import org.apache.spark.mllib.feature.IDFModel
import org.apache.spark.ml.feature.Tokenizer

/**
 * Created by dayue on 2017/5/29.
 */
object BinaryClassificationSpam extends App {

  val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
  .setMaster("local")

  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  case class Document(label:String, content:String)

  val docs = sc.textFile("data/").map{line=>
    val words = line.split("\t")
    Document(words.head.trim, words.tail.mkString(" "))
  }

  val labeledPointsUsingStanfordNLPRdd = getLabeledPoints(docs,"STANFORD")
  val lpTfIdf=withIdf(labeledPointsUsingStanfordNLPRdd).cache()

  //Split dataset
  val spamPoints = lpTfIdf.filter(point => point.label == 1).randomSplit(Array(0.8, 0.2))
  val hamPoints = lpTfIdf.filter(point => point.label == 0).randomSplit(Array(0.8, 0.2))

  println("Spam count:" + (spamPoints(0).count) + "::" + (spamPoints(1).count))
  println("Ham count:" + (hamPoints(0).count) + "::" + (hamPoints(1).count))

  val trainingSpamSplit = spamPoints(0)
  val testSpamSplit = spamPoints(1)

  val trainingHamSplit = hamPoints(0)
  val testHamSplit = hamPoints(1)

  val trainingSplit = trainingSpamSplit ++ trainingHamSplit
  val testSplit = testSpamSplit ++ testHamSplit


  val logisticWithSGD = getAlgorithm("LOGSGD", 100, 1, 0.001)
  val logisticWithBfgs = getAlgorithm("LOGBFGS", 100, 1, 0.001)
  val svmWithSGD = getAlgorithm("SVMSGD", 100, 1, 0.001)





  def runClassification(algorithm :GeneralizedLinearAlgorithm[_ <:GeneralizedLinearModel],
                         trainData:RDD[LabeledPoint],testData:RDD[LabeledPoint]):RDD[(Double,Double)]={

    val model = algorithm.run(trainData)
    val predicted = model.predict(testData.map(point => point.features))
    val actuals = testData.map(point => point.label)
    val predictAndActuals:RDD[(Double,Double)] = predicted.zip(actuals)

    predictAndActuals

  }



  def getAlgorithm(algo:String,iterations:Int, stepSize:Double,regParam:Double) = algo match{

    case "LOGSGD" => {
      val algo = new LogisticRegressionWithSGD()
      algo.setIntercept(true).optimizer.setNumIterations(iterations).setStepSize(stepSize).setRegParam(regParam)
      algo
    }
    case "LOGBFGS" => {
      val algo = new LogisticRegressionWithLBFGS()
      algo.setIntercept(true).optimizer.setNumIterations(iterations).setRegParam(regParam)
      algo
    }
    case "SVMSGD" => {
      val algo = new SVMWithSGD()
      algo.setIntercept(true).optimizer.setNumIterations(iterations).setStepSize(stepSize).setRegParam(regParam)
      algo
    }

  }



  def withIdf(lPoints: RDD[LabeledPoint]): RDD[LabeledPoint] = {
    val hashedFeatures = lPoints.map(lp => lp.features)
    val idf: IDF = new IDF()
    val idfModel: IDFModel = idf.fit(hashedFeatures)

    val tfIdf: RDD[Vector] = idfModel.transform(hashedFeatures)

    val lpTfIdf= lPoints.zip(tfIdf).map {
      case (originalLPoint, tfIdfVector) => {
        new LabeledPoint(originalLPoint.label, tfIdfVector)
      }
    }

    lpTfIdf
  }


  def getLabeledPoints(docs: RDD[Document], library: String): RDD[LabeledPoint] = library match {

    case "EPIC" => {

      //Use Scala NLP - Epic
      val labeledPointsUsingEpicRdd: RDD[LabeledPoint] = docs.mapPartitions { docIter =>

        val segmenter = MLSentenceSegmenter.bundled().get
        val tokenizer = new TreebankTokenizer()
        val hashingTf = new HashingTF(5000)

        docIter.map { doc =>
          val sentences = segmenter.apply(doc.content)
          val tokens = sentences.flatMap(sentence => tokenizer(sentence))

          //consider only features that are letters or digits and cut off all words that are less than 2 characters
          val filteredTokens=tokens.toList.filter(token => token.forall(_.isLetterOrDigit)).filter(_.length() > 1)

          new LabeledPoint(if (doc.label.equals("ham")) 0 else 1, hashingTf.transform(filteredTokens))
        }
      }.cache()

      labeledPointsUsingEpicRdd

    }

    case "STANFORD" => {
      def corePipeline(): StanfordCoreNLP = {
        val props = new Properties()
        props.put("annotators", "tokenize, ssplit, pos, lemma")
        new StanfordCoreNLP(props)
      }

      def lemmatize(nlp: StanfordCoreNLP, content: String): List[String] = {
        //We are required to prepare the text as 'annotatable' before we annotate :-)
        val document = new Annotation(content)
        //Annotate
        nlp.annotate(document)
        //Extract all sentences
        val sentences = document.get(classOf[SentencesAnnotation]).asScala

        //Extract lemmas from sentences
        val lemmas = sentences.flatMap { sentence =>
          val tokens = sentence.get(classOf[TokensAnnotation]).asScala
          tokens.map(token => token.getString(classOf[LemmaAnnotation]))

        }
        //Only lemmas with letters or digits will be considered. Also consider only those words which has a length of at least 2
        lemmas.toList.filter(lemma => lemma.forall(_.isLetterOrDigit)).filter(_.length() > 1)
      }

      val labeledPointsUsingStanfordNLPRdd: RDD[LabeledPoint] = docs.mapPartitions { docIter =>
        val corenlp = corePipeline()
        val stopwords = Source.fromFile("stopwords.txt").getLines()
        val hashingTf = new HashingTF(5000)

        docIter.map { doc =>
          val lemmas = lemmatize(corenlp, doc.content)
          //remove all the stopwords from the lemma list
          lemmas.filterNot(lemma => stopwords.contains(lemma))

          //Generates a term frequency vector from the features
          val features = hashingTf.transform(lemmas)

          //example : List(until, jurong, point, crazy, available, only, in, bugi, great, world, la, buffet, Cine, there, get, amore, wat)
          new LabeledPoint(
            if (doc.label.equals("ham")) 0 else 1,
            features)

        }
      }.cache()

      labeledPointsUsingStanfordNLPRdd
    }

  }



}
