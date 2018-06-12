package com.kyrie.utils

import java.text.SimpleDateFormat
import java.util.{Calendar, GregorianCalendar, Date}
import scala.collection.mutable

/**
 * Created by Tercel on 2016/10/14.
 */
object DateUtils {


  def parseDate(str:String,pattern:String="yyyy-MM-dd"):Date = {
    val sf = new SimpleDateFormat(pattern)
    sf.parse(str)
  }

  def formatDate(date:Date,pattern:String="yyyy-MM-dd"):String = {
    val sf = new SimpleDateFormat(pattern)
    sf.format(date)
  }

  def yesteryday(date:String,pattern:String="yyyy-MM-dd"):String = {
    val sf = new SimpleDateFormat(pattern)
    val d = sf.parse(date)
    val calendar = Calendar.getInstance();
    calendar.setTime(d)
    calendar.add(Calendar.DATE,-1);

    sf.format(calendar.getTime)
  }

  def getMaxWeekNumOfYear(year:Int) : Int={
    val c = new GregorianCalendar();
    c.set(year, Calendar.DECEMBER, 31, 23, 59, 59);
    c.setFirstDayOfWeek(Calendar.MONDAY);
    c.setMinimalDaysInFirstWeek(7);
    c.get(Calendar.WEEK_OF_YEAR)
  }

  def rangeWeek(start:String,end:String) :Set[String]={
    var Array(startYear,startWeekStr) = start.split("-")
    var Array(endYear,endWeekStr) = end.split("-")
    var startWeek = startWeekStr.toInt
    var endWeek = endWeekStr.toInt
    var set:Set[String] = Set()
    if(startYear == endYear){
      while(startWeek <= endWeek){
        set+=(s"$startYear-$startWeek")
        startWeek += 1
      }
    } else {
      val lastWeek  = DateUtils.getMaxWeekNumOfYear(startYear.toInt)
      while(startWeek <= lastWeek){
        set+=(s"$startYear-$startWeek")
        startWeek += 1
      }
      var firstWeek = 1
      while(firstWeek <= endWeek){
        set+=(s"$endYear-$firstWeek")
        firstWeek += 1
      }
    }
    set
  }

  def rangeHour(start:String,end:String):Set[String] = {
    val istart = Integer.parseInt(start)
    val iend = Integer.parseInt(end)

    (istart to iend).map{i =>
      val is = i.toString
      if (is.length == 1) s"0$is" else is
    }.toSet
  }

  def rangeMonth(start:String ,end:String):Set[String] = {
    var Array(startYear,startMonthStr) = start.split("-")
    var Array(endYear,endMonthStr) = end.split("-")

    var startMonth = startMonthStr.toInt
    var endMonth = endMonthStr.toInt
    var set:Set[String] = Set()
    if(startYear == endYear){
      while(startMonth <= endMonth){
        set+=(s"$startYear-$startMonth")
        startMonth += 1
      }
    } else {
      val lastMonth  = 12
      while(startMonth <= lastMonth){
        set+=(s"$startYear-$startMonth")
        startMonth += 1
      }
      var firstMonth = 1
      while(firstMonth <= endMonth){
        set+=(s"$endYear-$firstMonth")
        firstMonth += 1
      }
    }
    set

  }


  def main(args: Array[String]) {

    println( rangeHour("01" ,"05"))

    //    println (getMaxWeekNumOfYear(2014))

    //    println (rangeWeek("2016-49","2017-3").toString())

    println(rangeMonth("2016-2","2017-9"))

  }
}
