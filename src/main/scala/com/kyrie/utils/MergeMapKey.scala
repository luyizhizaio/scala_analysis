package com.kyrie.utils

import java.util

import org.apache.log4j.{Level, Logger}
import scala.collection.JavaConversions._

import scala.collection.mutable

/**
 * Created by tend on 2017/8/17.
 */
object MergeMapKey {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR)


    val list = new util.ArrayList[util.Map[String, AnyRef]]()



    val map1 = Map("metaid" -> "2", "typeid" -> "22")
    val map2 = Map("metaid" -> "3", "typeid" -> "22")
    val map3 = Map("metaid" -> "4", "typeid" -> "22")
    val map4 = Map("metaid" -> "4", "typeid" -> "24")
    val map5 = Map("metaid" -> "6", "typeid" -> "25")

    list.add(map1)
    list.add(map2)
    list.add(map3)
    list.add(map4)
    list.add(map5)



    val newMap = mutable.Map.empty[String, List[String]]

    list.toIterator.foreach {
      case map =>
        val metaId = map.get("metaid").toString
        val typeId = map.get("typeid").toString
        //          Some(typeId -> List(metaId))
        if (newMap.get(typeId).isEmpty) {
          newMap += (typeId -> List(metaId))
        } else {
          val list = newMap.get(typeId).get :+ metaId
          newMap += (typeId -> list)
        }
    }
    newMap.map { case (k, v) => k -> v.mkString(",") }.toMap.foreach { v => println(v._1 + "sssss" + v._2) }


  }
}