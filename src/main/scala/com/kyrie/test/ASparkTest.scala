package com.kyrie.test

import java.util
import java.util.Calendar

import org.apache.log4j.{Level, Logger}

import scala.collection.JavaConversions._
import scala.collection.immutable.TreeMap
import scala.collection.mutable

/**
 * Created by tend on 2017/5/2.
 */
object ASparkTest {


  case class MSCParam(mediaid:String, campaignid:String, idtype:String)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR)


    val list = new util.ArrayList[util.Map[String,AnyRef]]()



    val map1 = Map("metaid"->"2","typeid"->"22")
    val map2 = Map("metaid"->"3","typeid"->"22")
    val map3 = Map("metaid"->"4","typeid"->"22")
    val map4 = Map("metaid"->"4","typeid"->"24")
    val map5 = Map("metaid"->"6","typeid"->"25")

    list.add(map1)
    list.add(map2)
    list.add(map3)
    list.add(map4)
    list.add(map5)



      val newMap = mutable.Map.empty[String,List[String]]

    list.toIterator.foreach{
        case map =>
          val metaId = map.get("metaid").toString
          val typeId = map.get("typeid").toString
          //          Some(typeId -> List(metaId))
          if(newMap.get(typeId).isEmpty){
            newMap += (typeId ->List(metaId))
          }else{
            val list = newMap.get(typeId).get :+ metaId
            newMap += (typeId -> list)
          }
      }
      newMap.map{case(k,v)=> k -> v.mkString(",")}.toMap.foreach{v => println(v._1 +"sssss" +v._2)}




//    func(list).foreach(v => println(v._2))


    /*val reqMap = new util.HashMap[String, AnyRef]()

    reqMap.put("xxxx","==========")

    val customerJson = new util.HashMap[String, String]();

    println(s"matchService MQ SendTopic taskId=,...before..Send=${new Gson().toJson(reqMap)}");*/



    // sh /home/mc
//    ShellUtils.callShell("sh /home/mcloud/datacenter_test/job/test5.sh aaa\\;bbb ccc",true)

   /* val a = (0 to 15).map{n =>

      val len = n.toString.length
      val sub =if(len < 2){
        s"0${n}.gz"
      }else s"${n}.gz"

      s"${sub}"
    }.mkString(";")

    println(a)

    println((0 to 15).mkString("'","','","'"))
*/
/*
    val Array(ta_numerator,ta_denominator,tdta_denominator) = if(args.length >1) args(1).split(",") else " , , ".split(",")

    println(ta_numerator)*/


    val ss = "2017-05-15_2017-07-01_8ztpFE99"

    val arr =  ss.split("_")

    println(s"${arr(0)}_${arr(1)}")

    /*val s="{'campaignid':'fuck'}"

    val mscParam = new Gson().fromJson(s,classOf[MSCParam])
    println(mscParam.campaignid)*/



//    val conf = new SparkConf().setAppName("bitmap instal active merge").setMaster("local")
//    val sc = new SparkContext(conf)
//    val sqlContext = new SQLContext(sc)
//
//    val df  =sqlContext.createDataFrame(Seq(("asssssssssssssssss","1"),("bddddddddddddddddddddddddddddd","2")))
//
//    println("-----------------------------------------------------------------")
//    df.map(_.mkString(",")).take(10).foreach(println)
//
//    println("-----------------------------------------------------------------")


    /*val a = "111222|33333|3444444"
    a.split("\\|").foreach(println)*/


    /*val days = DateUtil.range("2017-04-11","2017-06-08","yyyy-MM-dd").toSet

    days.foreach(println(_))*/


//    BitmapLoader.loadBitmap("", sc, Set.empty[String]).map(_._2).map(bitmap => bitmap.i)

//    val start ="2017-04-17"
//    val end = "2017-05-14"
//    val key ="model"
//    getPaths(start,end,key).foreach(println(_))

    /*var map = Map.empty[Int, Seq[String]]
    map += 10->"25.05089_118.14766_1,25.03853_117.96776_2,25.03716_117.92519_1".split(",").toSeq
    map += 9-> "25.03716_117.92519_1,25.05089_118.14766_1".split(",").toSeq
    map += 11->"25.05089_118.14766_1,25.03853_117.96776_1,25.03716_117.92519_1".split(",").toSeq
    println(map.size)

    println(location_filter(map,workTimeFilter))*/



//   val r = sum(1 to 5 :_*)
//
//    println(r)
//
//    val r2 = sum(Seq(1,2,3,4,5):_*)
//    println(r2)
//
//    val paths = (10 to 19).map{n =>
//      s"/datascience/etl2/extract/ta/2017/05/${n}/*"
//    }
//    println(paths.mkString(";"))
//
//
//    val ta = sqlContext.read.parquet(paths:_*)


   /* val now = System.currentTimeMillis();
    val calendar = Calendar.getInstance();
    calendar.setTimeInMillis(1484668800000L);
    System.out.println(now + " = " + DateUtils.formatDate(calendar.getTime()));
    System.out.println(now + " hour = " + DateUtils.formatDate(calendar.getTime(),"HH"));
*/

    /*val s=URLEncoder.encode("SAMSUNG:SM-A7009","UTF-8");

    System.out.println("ecode s="+s);
    System.out.println("decode s="+ URLDecoder.decode("SAMSUNG:SM-A7009","UTF-8"));*/


//    def isIntByRegex(s : String) = {
//      val pattern = """^(\d+)$""".r
//      s match {
//        case pattern(_*) => true
//        case _ => false
//      }
//    }
//    println(isIntByRegex("-1"))
//      println(isIntByRegex("a123"))
//      println(isIntByRegex("123z"))
//      println(isIntByRegex("12m3"))

    /**
     356405054813804
864297034468733
861069031825832
     */

    /*println(Md5Util.MD5("356405054813804"))
    println(Md5Util.MD5("864297034468733"))
    println(Md5Util.MD5("861069031825832"))*/

//    println(createOutPut("/xxx/xxx"))
  }





  def createOutPut(path:String) :Seq[String] ={

    (1 to 16).map{n =>

      val len = n.toString.length
      val sub =if(len < 2){
        s"0${n}.gz"
      }else s"${n}.gz"

      s"${path}/${sub}"
    }
  }

  def sum(params:Int*)={
    var result=0
    for(par <- params){
      result += par
    }
    result
  }



  def workTimeFilter(hour:Int):Boolean = {
    val start_time = 9
    val end_time = 17
    hour >= start_time && hour <= end_time
  }


  def location_filter(set: Map[Int, Seq[String]],rule:Int => Boolean): String = {
    val map = set.filter(x => rule(x._1)).flatMap(_._2).map{
      l =>
        val temp = l.split("\\_")
        temp(2).toInt -> (temp(0) + "_" + temp(1))
    }.toMap

    val sortedMap = TreeMap.empty[Int,String] ++ map
    if(sortedMap.size > 0) sortedMap.last._2 else ""
  }



//  def weekPath(key:String,week:String) = s"/mcloud/data/datacenter/bitmap/week/$key/$week"

}
