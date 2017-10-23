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
    var t=15
    //潜在感兴趣用户的数量
    val k = 5

    val ε = 0.04
    //本地簇的大小
    val b = 5
    //节点数
    val n = 15

    val c4 = 140

    val l = 1.0

    val idRDD = andiAlgr(sc,data,t,seed,k,n,ε,b,l,c4)
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

  def andiAlgr(sc:SparkContext,data:RDD[String],t:Int,seed:List[Int],k:Int,n:Int,ε:Double,b:Int,l:Double,c4:Int):RDD[Long]={

    //邻接矩阵
    val adjMatrix = geneAdjMatrix(data)
    //权重-节点的度
    val degrees:VertexRDD[Int] = geneBinaryBipartiteGraphWeight(data).cache()

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
    }).toBlockMatrix().persist(StorageLevel.MEMORY_AND_DISK)

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
      println(s"qt matrix: ${qt.numRows()}")
      printMatrix(qt.toCoordinateMatrix())


      val entries: RDD[MatrixEntry] = qt.toCoordinateMatrix().entries.cache()

      val rMatrixEntry2 = degrees.leftOuterJoin(entries.map{entry => entry.i -> entry.value}).map{v =>

        val degree = v._2._1
        val p = v._2._2.getOrElse(0.0)

        if (p < degree * ε) MatrixEntry(v._1,0 ,0.0)
        else MatrixEntry(v._1,0 ,p)
      }


      rMatrix = new CoordinateMatrix(rMatrixEntry2).toBlockMatrix()

      println(s"r Matrix after: ${rMatrix.numRows()}")
      printMatrix(rMatrix.toCoordinateMatrix())

      //Size
      if(entries.count() <= k){
        return entries.map{entry => entry.i}
      }

      val qtRdd = entries.map{entry => entry.i -> entry.value}.cache

      for ( j <- k to n ){

        //Volume 判断条件：节点的值除以节点的度得到一个值，根据这个值排序，取出前j个节点,前j个节点的度的和
        val topQt = degrees.join(qtRdd).map{x  =>
          val degree = x._2._1
          val p =x._2._2
          (x._1 , p / degree ,degree)
        }.top(j)(QtPreDef.tupleOrdering)

        val lambda = topQt.map{tri => tri._3}.reduce(_ + _)
        println(s"lamda:$lambda")

        //Large Prob Mass ,I= qt中j'节点的值除以j'节点的度。
        val I = topQt.last._2

        val f = ((l+2) * Math.pow(2,b))/c4
        println(s"I=$I ,f:$f" )

        if(lambda >= Math.pow(2,b) && lambda < vol * 5/6 && I >= f) {
          //排序r获取前j个元素生成Sj(qt)
          val Sj = topQt.take(k)

          return sc.parallelize(Sj).map{case(id,_,_) => id}
        }

      }

      index += 1
    }

    return sc.parallelize(Seq())

  }

  def getLambda(qtRdd:RDD[(Long,Double)], degrees:VertexRDD[Int],j:Int):Int={

    //判断条件：节点的值除以节点的度得到一个值，根据这个值排序，取出前j个节点,前j个节点的度的和
    degrees.join(qtRdd).map{x  =>
      val degree = x._2._1
      val p =x._2._2
      (x._1 , p / degree ,degree)
    }.top(j)(QtPreDef.tupleOrdering).map{tri => tri._3}.reduce(_ + _)

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
