package com.kyrie.andi2

/**
 * Created by tend on 2017/10/23.
 */
object QtPreDef {


  implicit val tupleOrdering = new Ordering[(Long,Double,Int)] {
    def compare(a: (Long,Double,Int), b: (Long,Double,Int)) = a._2 compareTo b._2
  }

  implicit val tupleOrderingDouble = new Ordering[(Long,Double,Double)] {
    def compare(a: (Long,Double,Double), b: (Long,Double,Double)) = a._2 compareTo b._2
  }



}
