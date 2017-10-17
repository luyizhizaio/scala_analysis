package com.kyrie.andi

import java.text.DecimalFormat

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{VertexRDD, Graph, Edge}
import org.apache.spark.mllib.linalg.distributed.{RowMatrix, CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.linalg.{Vector, Vectors}

import breeze.linalg._

/**
 * Created by tend on 2017/10/9.
 */
object ANDIalgr {



  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    val seed = List(1,10)

    var t=20


    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)

    val sc = new SparkContext(conf)

    val data:RDD[String] = sc.textFile("data/edge")

    andiAlgr(t,seed,data)


  }

  def andiAlgr(t:Int,seed:List[Int],data:RDD[String])={


    val adjMatrixEntry1 = data.map(_.split(" ") match { case Array(id1 ,id2) =>
      MatrixEntry(id1.toLong -1,id2.toLong-1 , 1.0)
    })

    val adjMatrixEntry2 = data.map(_.split(" ") match { case Array(id1 ,id2) =>
      MatrixEntry(id2.toLong -1,id1.toLong-1 , 1.0)
    })

    val adjMatrixEntry = adjMatrixEntry1.union(adjMatrixEntry2)
    //邻接矩阵
    var adjMatrix = new CoordinateMatrix(adjMatrixEntry).toBlockMatrix()


    val edgeRdd = data.map{_.split(" ") match {case Array(id1,id2) =>
      Edge(id1.toLong -1 ,id2.toLong -1,None)
    }}

    val graph = Graph.fromEdges(edgeRdd,0)
    val degrees: VertexRDD[Int] = graph.degrees

    //对角矩阵的逆矩阵
    val diagMatrixInverseEntry = degrees.map{case(id,degree) => MatrixEntry(id,id,1/degree)}
    val diagInverseMatrix = new CoordinateMatrix(diagMatrixInverseEntry)


    //单位阵
    val identityMatrixEntry = degrees.map{case(id,_) => MatrixEntry(id,id,1)}
    val identityMatrix = new CoordinateMatrix(identityMatrixEntry)


    val matrix = adjMatrix.multiply(diagInverseMatrix.toBlockMatrix()).add(identityMatrix.toBlockMatrix())
/*
    val M = new CoordinateMatrix(matrix.toCoordinateMatrix().entries.map{entry =>
      val df=new DecimalFormat("0.00");
      val a = entry.value * 0.5
      MatrixEntry(entry.i,entry.j ,df.format(a).toDouble)
    }).toBlockMatrix()*/


    val M = new CoordinateMatrix(matrix.toCoordinateMatrix().entries.map{entry =>

      MatrixEntry(entry.i,entry.j ,entry.value * 0.5)
    }).toBlockMatrix()

    println("M matrix")
    printMatrix(M.toCoordinateMatrix())


    val rMatrixEntry = degrees.map{case(id,_) =>

      if(seed.contains(id))
        MatrixEntry(id.toLong, 0, 1.0)
      else
        MatrixEntry(id.toLong, 0, 0.0)

    }

    var rMatrix = new CoordinateMatrix(rMatrixEntry).toBlockMatrix()

    println("rMatrix Num of row=" + rMatrix.numRows() +", Num of column=" +rMatrix.numCols())
    printMatrix(rMatrix.toCoordinateMatrix())

    var index =0
    while( index < t){

      rMatrix =  M.multiply(rMatrix)

      println(s"r Matrix index =$index")
      printMatrix(rMatrix.toCoordinateMatrix())

      index = index +1
    }

  }


  def printMatrix(matrix :CoordinateMatrix): Unit ={

    matrix.entries.foreach(entry => println( "row:" + entry.i + "     column:" + entry.j + "   value:" + entry.value))
  }


}
