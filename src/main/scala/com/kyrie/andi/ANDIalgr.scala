package com.kyrie.andi


import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{VertexRDD, Graph, Edge}
import org.apache.spark.mllib.linalg.distributed.{BlockMatrix, CoordinateMatrix, RowMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.linalg.{Vector, Vectors}

/**
 * Created by tend on 2017/10/9.
 */
object ANDIalgr {

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    val data:RDD[String] = sc.textFile("data/edge")
    //活动节点
    val seed = List(0,2)
    //迭代次数
    var t=30
    //潜在感兴趣用户的数量
    val k = 5

    val ε = 0.04
    //本地簇的大小
    val b = 3
    //节点数
    val n = 15

    val c4 = 140

    val idRDD = andiAlgr(sc,data,t,seed,k,n,ε,b)
    println("result：")
    idRDD.foreach(println)

  }

  /**
   * 生成单位阵
   * @param sc
   * @param n
   * @return
   */
  def geneIdentityMatrix(sc:SparkContext,n:Int) ={

    val identityMatrixEntry = sc.parallelize(0 to n-1).map{id => MatrixEntry(id,id,1.0)}
    new CoordinateMatrix(identityMatrixEntry)

  }

  def andiAlgr(sc:SparkContext,data:RDD[String],t:Int,seed:List[Int],k:Int,n:Int,ε:Double,b:Int):RDD[Long]={

    //邻接矩阵
    val adjMatrix = geneAdjMatrix(data)
    //权重
    val degrees:VertexRDD[Int] = geneBinaryBipartiteGraphWeight(data)

    degrees.cache()

    val vol =  degrees.map{v => v._2}.reduce(_ + _).toDouble
    println(s"vol :$vol")


    //对角矩阵的逆矩阵
    val diagMatrixInverseEntry = degrees.map{case(id,degree) => MatrixEntry(id,id,1/degree.toFloat)}
    val diagInverseMatrix = new CoordinateMatrix(diagMatrixInverseEntry)

    //单位阵
    val identityMatrix = geneIdentityMatrix(sc,n)

    val matrix = adjMatrix.multiply(diagInverseMatrix.toBlockMatrix()).add(identityMatrix.toBlockMatrix())

    val M = new CoordinateMatrix(matrix.toCoordinateMatrix().entries.map{entry =>
      MatrixEntry(entry.i,entry.j ,entry.value * 0.5)
    }).toBlockMatrix()

    M.persist(StorageLevel.MEMORY_AND_DISK)

    println("M matrix")
    println(M.toLocalMatrix())
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
      val qt =  M.multiply(rMatrix)
      println("qt matrix:")
      printMatrix(qt.toCoordinateMatrix())


      println(s"r Matrix before: ${rMatrix.numRows()}")
      printMatrix(rMatrix.toCoordinateMatrix())

      rMatrix = new CoordinateMatrix(qt.toCoordinateMatrix().entries.map{entry =>
        if(entry.value < ε) MatrixEntry(entry.i,entry.j ,0.0)
        else entry
      }).toBlockMatrix()

      println(s"r Matrix after: ${rMatrix.numRows()}")
      printMatrix(rMatrix.toCoordinateMatrix())

      //排序
      val sortedR = rMatrix.toCoordinateMatrix().entries.map(entry => entry.i ->entry.value ).sortBy(x => x._2,false)

      //Size

      if(rMatrix.numRows() <= k){
        return rMatrix.toCoordinateMatrix().entries.map{entry => entry.i}
      }


      for ( j <- k to n ){

        //Volume
        //判断条件：节点的值除以节点的度得到一个值，根据这个值去除前j个节点,前j个节点的度的和
        val qtRdd = qt.toCoordinateMatrix().entries.map{entry => entry.i}
        val lambda = getLambda(qtRdd,degrees).toDouble
        println(s"lamda:$lambda")

        //Large Prob Mass
        //j'满足λj 0 (qt ) ≤ 2b ≤ λj 0+1(qt ) 这个式子， I= qt中j'节点的值除以j'节点的度。


        /*if(lambda >= Math.pow(2,b) && lambda < vol * 5/6  ) {

          //排序r获取前j个元素生成Sj(qt)
          val Sj = sortedR.take(k)

          return sc.parallelize(Sj).map{case(id,weight) => id}
        }*/
      }

      index += 1
    }

    return sc.parallelize(Seq())

  }

  def getLambda(qt:RDD[Long], degrees:VertexRDD[Int]) :Int={

    val qt1 = qt.map{id => (id -> 0)}
    degrees.join(qt1).map{x  => x._2._1}.reduce(_ + _)
  }



  def geneAdjMatrix(data:RDD[String]) :BlockMatrix = {

    val adjMatrixEntry1 = data.map(_.split(" ") match { case Array(id1 ,id2) =>
      MatrixEntry(id1.toLong,id2.toLong , 1.0)
    })

    val adjMatrixEntry2 = data.map(_.split(" ") match { case Array(id1 ,id2) =>
      MatrixEntry(id2.toLong,id1.toLong , 1.0)
    })

    val adjMatrixEntry = adjMatrixEntry1.union(adjMatrixEntry2)
    //邻接矩阵
    val adjMatrix = new CoordinateMatrix(adjMatrixEntry).toBlockMatrix()
    adjMatrix
  }

  //binary bipartite graphs
  def geneBinaryBipartiteGraphWeight(data:RDD[String]):VertexRDD[Int]={

    val edgeRdd = data.map{_.split(" ") match {case Array(id1,id2) =>
      Edge(id1.toLong ,id2.toLong,None)
    }}

    val graph = Graph.fromEdges(edgeRdd,0)
    val degrees: VertexRDD[Int] = graph.degrees
    degrees
  }

  //continuous bipartite graphs
  def geneContinuousBipartiteGraphWeight()={

  }

  def printMatrix(matrix :CoordinateMatrix): Unit ={

    matrix.entries.foreach(entry => println( "row:" + entry.i + "     column:" + entry.j + "   value:" + entry.value))
  }


}
