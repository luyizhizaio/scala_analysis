package com.kyrie.andi

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{VertexRDD, Edge, Graph}
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by tend on 2017/10/17.
  */
object ANDIalgrTest2 {

   def main(args: Array[String]) {

     Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
     Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);


     val seeds  = List(1,2)


     val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)

     val sc = new SparkContext(conf)

     val data:RDD[String] = sc.textFile("data/0.edges")



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








     val rMatrixEntry = graph.vertices.map{case(id,_) =>

       if(seeds.contains(id))
         MatrixEntry(id.toLong, 0, 1.0)
       else
         MatrixEntry(id.toLong, 0, 0.0)

     }

     var rMatrix = new CoordinateMatrix(rMatrixEntry).toBlockMatrix()

     printMatrix(rMatrix.toCoordinateMatrix())

     var t = 0
     while(t < 19){
       rMatrix = M.multiply(rMatrix)

       println(s"index=$t")
       printMatrix(rMatrix.toCoordinateMatrix())

       t = t+1
     }

   }


   def printMatrix(matrix :CoordinateMatrix): Unit ={

     matrix.entries.foreach(entry => println( "row:" + entry.i + "     column:" + entry.j + "   value:" + entry.value))
   }


 }
