package com.talkingdata.dtasset.unilever

import com.talkingdata.addmp.tools.Hdfs
import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sql._

/**
 * Created by tend on 2018/4/28.
 */
object DataAssetToElasticSearch {


  def main(args: Array[String]) {

    val Array(esStdInput,esindex,estype) = args(0).split(";")


    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    /*conf.set("es.nodes", ip)
    conf.set("es.port", "9200")
    conf.set("es.cluster.name",clusterName)
    conf.set("es.index.auto.create", "true")*/
    //Bailing out...错误
    //    conf.set("es.batch.size.entries", "50")
    //插入失败后无限重复插数据
    //    conf.set("es.batch.write.retry.count", "-1")
    //查数据等待时间
    //    conf.set("es.batch.write.retry.wait", "100")

    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).getOrCreate()


    Hdfs.files(esStdInput).foreach{line =>

      val name = line.getName
      if(!name.startsWith("_")){
        val index = name.split("=")(1)
        val ds = spark.read.parquet(s"$esStdInput/$name").select("campaign_id","std_time","std_brand","action","offset","std_cnt","batch_no")

        EsSparkSQL.saveToEs(ds,s"${esindex}_$index/$estype",Map("es.mapping.routing" -> "offset"))
      }


    }





  }

}
