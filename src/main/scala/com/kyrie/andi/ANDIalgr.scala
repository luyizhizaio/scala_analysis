package com.kyrie.andi


import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{VertexRDD, Graph, Edge}
import org.apache.spark.mllib.linalg.distributed.{BlockMatrix, CoordinateMatrix, RowMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}
import util.control.Breaks._

/**
 * Created by tend on 2017/10/9.
 */
object ANDIalgr {


  val c4 = 140.0


  case class Param(seed:List[Int],k:Int,epsilon:Int,b:Int,n:Int,l:Int,u:Int)

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    val data:RDD[String] = sc.textFile("data/edge2")
    //活动节点
    val seed = List(2,3)
    //迭代次数
    var t=100
    //潜在感兴趣用户的数量
    val k = 2

    val epsilon = 0.1
    //本地簇的大小-1
    val b = 1
    //节点数
    val n = 4

    val l = 4.0

    //用户的总数量
    val u =2

    val idRDD = andiAlgr(sc,data,t,seed,k,n,epsilon,b,l,u)
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

  def andiAlgr(sc:SparkContext,data:RDD[String],t:Int,seed:List[Int],k:Int,n:Int,epsilon:Double,b:Int,l:Double,u:Int):RDD[Long]={

    //邻接矩阵
    val adjMatrix = geneAdjMatrix(data)
    //权重-节点的度
    val degrees:RDD[(Long,Int)] = geneBinaryBipartiteGraphWeight(data).cache()

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

        if (p < degree * epsilon) MatrixEntry(v._1,0 ,0.0)
        else MatrixEntry(v._1,0 ,p)
      }


      rMatrix = new CoordinateMatrix(rMatrixEntry2).toBlockMatrix()

      println(s"r Matrix after: ${rMatrix.numRows()}")
      printMatrix(rMatrix.toCoordinateMatrix())

      index += 1
      breakable{
        if(entries.count().toInt < k){
          break;
        }

        val qtRdd = entries.map{entry => entry.i -> entry.value}.cache

        val qt = degrees.join(qtRdd).map{x  =>
          val degree = x._2._1
          val p =x._2._2
          (x._1 , p / degree ,degree)
        }
        //用户数
        var users:Int = 0
        for ( j <- k to n-1 ){


          //Volume 判断条件：节点的值除以节点的度得到一个值，根据这个值排序，取出前j个节点,前j个节点的度的和
          val topQt = qt.top(j+1)(QtPreDef.tupleOrdering)
          topQt.foreach(println(_))
          users = topQt.filter{case(index,_,_) => index < u}.length

          val lambda = topQt.map{tri => tri._3}.reduce(_ + _)
          println(s"lamda:$lambda")

          //Size
          val condition1 = users >= k

          val condition2 =  lambda >= (2 << b) && lambda < (vol * 5.0)/6

          //Large Prob Mass ,I= qt中j'节点的值除以j'节点的度。
          val I = topQt.last._2

          val f = (1/c4) * (l+2) * (2 << b)
          println(s"I=$I ,f:$f" )

          val condition3 = I >= f

          if(condition1 && condition2 && condition3) {
            //排序r获取前j个元素生成Sj(qt)
            val Sj = topQt.take(k)

            return sc.parallelize(Sj).map{case(id,_,_) => id}
          }

        }

      }
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

    val adjMatrixEntry1 = data.map(_.split(" ") match { case Array(id1 ,id2,click) =>
      MatrixEntry(id1.toLong,id2.toLong , click.toInt)
    })

    val adjMatrixEntry2 = data.map(_.split(" ") match { case Array(id1 ,id2,click) =>
      MatrixEntry(id2.toLong,id1.toLong , click.toInt)
    })

    val adjMatrixEntry = adjMatrixEntry1.union(adjMatrixEntry2)
    //邻接矩阵
    val adjMatrix = new CoordinateMatrix(adjMatrixEntry).toBlockMatrix()
    adjMatrix
  }

  //binary bipartite graphs
  def geneBinaryBipartiteGraphWeight(data:RDD[String]):RDD[(Long,Int)]={

    data.flatMap{_.split(" ") match {case Array(id1,id2,click) =>
      Seq((id1.toLong ,click.toInt),(id2.toLong,click.toInt))
    }}.reduceByKey(_ + _)

  }

  //continuous bipartite graphs
  def geneContinuousBipartiteGraphWeight()={

  }

  def printMatrix(matrix :CoordinateMatrix): Unit ={
    matrix.entries.foreach(entry => println( "row:" + entry.i + "     column:" + entry.j + "   value:" + entry.value))
  }


}
