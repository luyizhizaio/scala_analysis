package com.kyrie.scala.analysis.cookbook.chapter5

import java.util.Properties

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

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
