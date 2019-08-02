package com.myself

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Description: TODO
  * @Author: wanghailin
  * @Date: 2019/8/1
  */
object GraphXOperate {

  def main(args: Array[String]): Unit = {

    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    //设置允许环境
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    // 设置顶点和边，注意顶点和边都是用元组定义的Array
    // 顶点的数据类型是VD:(String,Int)
    val vertexArray = Array(
      (1L,("Alice",28)),
      (2L,("Jack",27)),
      (3L,("Leo",65)),
      (4L,("Tom",42)),
      (5L,("zhangsan",55)),
      (6L,("lisi",50))
    )
    // 边的数据类型 ED:Int
    val edgeArray = Array(
      Edge(2L,1L,7),
      Edge(2L,4L,2),
      Edge(3L,2L,4),
      Edge(3L,6L,3),
      Edge(4L,1L,1),
      Edge(5L,2L,2),
      Edge(5L,3L,8),
      Edge(5L,6L,3)
    )
    //构造点和边
    val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)
    // 构造图 Graph[VD,ED]
    val graph: Graph[(String, Int), Int] = Graph(vertexRDD,edgeRDD)

    /**
      * 图的属性
      */
    println("属性演示")
    println("******************************************************")
    println("找出图中年龄大于30的顶点")
    graph.vertices.filter{
      case (id,(name,age)) => age>30
    }.collect.foreach{
      case (id,(name,age))=> println(s"$name is $age")
    }
    //边操作：找出图中属性大于5的边
    println("找出边属性大于5的边")
    graph.edges.filter(e=>e.attr>5).collect().foreach(e=>println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))

    println("列出边属性>5的Triplets")
    for(triplet<-graph.triplets.filter(t=>t.attr>5).collect){
      println(s"${triplet.srcAttr._1} linkes ${triplet.dstAttr._1}")
    }

    println("找出图中最大的出度、入度、度数")
    /*def max(a:(VertexId,Int),b:(VertexId,Int)):(VertexId,Int)={
      if(a._2>b._2) a else b
    }*/
    println("max of outDegrees:"+graph.outDegrees.reduce((a,b)=>if(a._2>b._2) a else b) + "max of inDegrees:"+graph.inDegrees.reduce((a,b)=>if(a._2>b._2) a else b)
      +"max of Degrees:"+graph.degrees.reduce((a,b)=>if(a._2>b._2) a else b)
    )
    println()

    /**
      * 转换操作
      */
    println("转换操作***************")
    println("顶点的转换操作，顶点age+10")
    graph.mapVertices{
      case (id,(name,age))=>(id,(name,age+10))
    }.vertices.collect.foreach(println)
    println()
    println("边的转换操作，边的属性*2")
    graph.mapEdges(e=>e.attr*2).edges.collect().foreach(println)

    /**
      * 结构操作
      */
    println("结构操作")
    println("顶点年纪>30的子图：")
    val subGraph = graph.subgraph(vpred = (id,vd)=>vd._2>=30)
    println("子图所有顶点：")
    subGraph.vertices.collect().foreach(println)
    println("子图所有的边")
    subGraph.edges.collect().foreach(println)

  }

}
