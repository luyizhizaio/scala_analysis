package com.kyrie.andi

import java.text.DecimalFormat

import org.apache.spark.graphx.{Graph, Edge}
import org.apache.spark.mllib.linalg.distributed.{RowMatrix, CoordinateMatrix, MatrixEntry}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.linalg.{Vector, Vectors}

import breeze.linalg._

/**
 * Created by tend on 2017/10/9.
 */
object ANDIalgr {




  def main(args: Array[String]) {

    val seed = List(1,2)


    var t=0




    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)

    val sc = new SparkContext(conf)

    val data = sc.textFile("data/edge")

    val adjMatrixEntry1 = data.map(_.split(" ") match { case Array(id1 ,id2) =>
      MatrixEntry(id1.toLong -1,id2.toLong-1 , 1.0)
    })

    val adjMatrixEntry2 = data.map(_.split(" ") match { case Array(id1 ,id2) =>

      MatrixEntry(id2.toLong -1,id1.toLong-1 , 1.0)
    })

    val adjMatrixEntry = adjMatrixEntry1.union(adjMatrixEntry2)

    //邻接矩阵
    var adjMatrix = new CoordinateMatrix(adjMatrixEntry).toBlockMatrix()
    println("Num of nodes=" + adjMatrix.numCols() +", Num of edges=" +data.count())
    //  Num of nodes=347, Num of edges=5038

    println( adjMatrix.toLocalMatrix().toString)


    val edgeRdd = data.map{_.split(" ") match {case Array(id1,id2) =>
      Edge(id1.toLong -1 ,id2.toLong -1,None)
    }}



    val graph = Graph.fromEdges(edgeRdd,0)
    //对角矩阵的逆矩阵
    val diagMatrixInverseEntry = graph.degrees.map{case(id,degree) => MatrixEntry(id,id,1/degree)}
    val diagInverseMatrix = new CoordinateMatrix(diagMatrixInverseEntry)

    println("Num of nodes=" + diagInverseMatrix.numCols() +", Num of edges=" +data.count())

    //单位阵
    val identityMatrixEntry = graph.degrees.map{case(id,_) => MatrixEntry(id,id,1)}
    val identityMatrix = new CoordinateMatrix(identityMatrixEntry)


    val matrix = adjMatrix.multiply(diagInverseMatrix.toBlockMatrix()).add(identityMatrix.toBlockMatrix())

    val M = new CoordinateMatrix(matrix.toCoordinateMatrix().entries.map{entry =>
      val df=new DecimalFormat("0.00");
      val a = entry.value * 0.5
      MatrixEntry(entry.i,entry.j ,df.format(a).toDouble)}).toBlockMatrix()


    var r = Vectors.sparse(graph.degrees.count().toInt ,Array(1,2) ,Array(2,3))



    val rMatrix = new RowMatrix(sc.parallelize(Array(r)),graph.degrees.count(),1)





    while( t < 20){


      M.toCoordinateMatrix().toRowMatrix().multiply(rMatrix)



      t = t +1
    }

    println("result ::::::::::::::::::::::::")
    adjMatrix.toCoordinateMatrix().entries.foreach(entry => println( "row:" + entry.i + "     column:" + entry.j + "   value:" + entry.value))



    adjMatrix.toCoordinateMatrix().entries.filter{ case enter => seed.contains(enter.i)}







   /* //2.计算拉普拉斯矩阵
    //
    val rows = adjMatrix.toIndexedRowMatrix().rows

    val diagMatrixEntry = rows.map{row=>
      MatrixEntry(row.index ,row.index, row.vector.toArray.sum)
    }
    //计算拉普拉斯矩阵 L =D-S
    val laplaceMatrix = new CoordinateMatrix(sc.union(adjMatrixEntry, diagMatrixEntry))

    //计算拉普拉斯矩阵的特征列向量构成的矩阵(假设聚类个数是5)
    val eigenMatrix = laplaceMatrix.toRowMatrix().computePrincipalComponents(5)

    val nodes = eigenMatrix.transpose.toArray.grouped(5).toSeq
    val nodeSeq = nodes.map(node =>Vectors.dense(node))

    val nodeVectors = sc.parallelize(nodeSeq)

*/







  }

}
