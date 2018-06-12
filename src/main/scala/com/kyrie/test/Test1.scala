package com.kyrie.test

import com.kyrie.utils.DateUtils


/**
 * Created by dayue on 2017/5/25.
 */
object Test1 {


  def main(args: Array[String]) {



    val start = "00"

    println(Integer.parseInt(start))


    val a = Integer.parseInt(start)


    println("fuck scala")

    getPaths("2016-11","2017-8","channel").foreach(println)

  }


  def getPaths(start:String,end:String,key:String) : Seq[String] = {
    val months = DateUtils.rangeMonth(start,end)
    getPaths(months,key)
  }

  def getPaths(months:Set[String],key:String) : Seq[String] = {

    months.map{month =>
      monthPath(key,month)
    }.toSeq

  }

  // /mcloud/data/datacenter/bitmap/month/channel/2017-8
  def monthPath(key:String,month:String) = s"/mcloud/data/datacenter/bitmap/month/$key/$month"

}
