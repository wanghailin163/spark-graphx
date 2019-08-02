package com.myself

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Description: 图计算案例
  * @Author: wanghailin
  * @Date: 2019/7/30
  */
object SparkGraphX {

  def main(args: Array[String]): Unit = {

    //初始化Spark的上下文
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName(this.getClass.getName)
      // 采用Kryo序列化方式
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc: SparkContext = new SparkContext(conf)

    // 构建点的集合
    val rdd1: RDD[(Long, (String, Int))] = sc.makeRDD(Seq(
      (1L, ("zhangsan", 22)),
      (2L, ("lisi", 23)),
      (6L, ("wangwu", 24)),
      (9L, ("zhaoliu", 25)),
      (133L, ("tianqi", 26)),
      (16L, ("liba", 27)),
      (21L, ("liujiu", 28)),
      (44L, ("wangshi", 29)),
      (158L, ("leo", 30)),
      (5L, ("Jack", 31)),
      (7L, ("tom", 32))
    ))
    // 构建边的集合
    val rdd2: RDD[Edge[Int]] = sc.makeRDD(Seq(
      Edge(1L, 133L, 0),
      Edge(2L, 133L, 0),
      Edge(6L, 133L, 0),
      Edge(9L, 133L, 0),
      Edge(6L, 138L, 0),
      Edge(21L, 138L, 0),
      Edge(44L, 138L, 0),
      Edge(16L, 138, 0),
      Edge(5L, 158L, 0),
      Edge(7L, 158L, 0)
    ))
    //构建图
    val graph: Graph[(String, Int), Int] = Graph(rdd1,rdd2)
    //取出顶点
    val common = graph.connectedComponents().vertices  //("当前顶点","当前顶点连通的最小顶点")
    //计算出顶点id(最小值为顶点)
    common.foreach(println)
    common.join(rdd1).map{
      case (userid,(minid,(name,age)))=>(userid,List((name,age)))
    }
  }.reduceByKey(_:::_)

}
