package com.hwj.graphx

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import java.io
import java.io.PrintWriter

object spark {

  /**
    * 统计关系出现的次数
    *
    * @param sc
    * @param path ：边文件
    * @param num  ：关系数量阈值
    * @return
    */
  def edgeCount(sc: SparkContext, path: String, num: Int) = {
    val textFile = sc.textFile(path)
    val counts = textFile.map(word => (word, 1))
      .reduceByKey(_ + _).filter(_._2 > num)
    //    counts.collect().foreach(println)
    counts
  }

  /**
    * 构建图
    *
    * @param sc
    * @param path1 :顶点文件
    * @param path2 ：边文件
    * @param num   ：关系数量阈值
    */
  def creatGraph(sc: SparkContext, path1: String, path2: String, num: Int) = {
    val hero = sc.textFile(path1)
    val counts = edgeCount(sc, path2, num)

    val verticesAll = hero.map { line =>
      val fields = line.split(' ')
      (fields(0).toLong, fields(1))
    }

    val edges = counts.map { line =>
      val fields = line._1.split(" ")
      Edge(fields(0).toLong, fields(1).toLong, line._2) //起始点ID必须为Long，最后一个是属性，可以为任意类型
    }
    val graph_tmp = Graph.fromEdges(edges, 1L)
    val vertices = graph_tmp.vertices.leftOuterJoin(verticesAll).map(x => (x._1, x._2._2.getOrElse("")))
    val graph = Graph(vertices, edges)

    graph
  }

  /**
    * 输出为gexf格式
    *
    * @param g ：图
    * @tparam VD
    * @tparam ED
    * @return
    */
  def toGexf[VD, ED](g: Graph[VD, ED]) = {
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
      "<gexf xmlns=\"http://www.gexf.net/1.2draft\" version=\"1.2\">\n" +
      " <graph mode=\"static\" defaultedgetype=\"directed\">\n  " +
      "<nodes>\n " +
      g.vertices.map(v => "  <node id=\"" + v._1 + "\" label=\"" + v._2 + "\" />\n").collect().mkString +
      "</nodes>\n  " +
      "<edges>\n" +
      g.edges.map(e => "  <edge source=\"" + e.srcId + "\" target=\"" + e.dstId + "\" weight=\"" + e.attr + "\"/>\n").
        collect().mkString +
      "</edges>\n        </graph>\n      </gexf>"

  }

  /**
    * 找出度为1或2的点
    *
    * @param g
    * @tparam VD
    * @tparam ED
    * @return
    */
  def minDegrees[VD, ED](g: GraphOps[VD, ED]) = {
    //    g.degrees.filter(_._2<3).map(_._1).collect().mkString("\n")
    g.degrees.filter(_._2 < 3).map(_._1).collect().map(a => a.toInt)
  }

  /**
    * 使用连通组件找到孤岛人群
    *
    * @param g
    * @tparam VD
    * @tparam ED
    * @return
    */
  def isolate[VD, ED](g: GraphOps[VD, ED]) = {
    g.connectedComponents.vertices.map(_.swap).groupByKey().map(_._2).collect().mkString("\n")
  }

  /**
    * 合并2张图
    *
    * @param g1
    * @param g2
    * @return
    */
  def mergeGraphs(g1: Graph[String, Int], g2: Graph[String, Int]) = {
    val v = g1.vertices.map(_._2).union(g2.vertices.map(_._2)).distinct().zipWithIndex()

    def edgeWithNewVid(g: Graph[String, Int]) = {
      g.triplets.map(et => (et.srcAttr, (et.attr, et.dstAttr)))
        .join(v)
        .map(x => (x._2._1._2, (x._2._2, x._2._1._1)))
        .join(v)
        .map(x => new Edge(x._2._1._1, x._2._2, x._2._1._2))
    }

    Graph(v.map(_.swap), edgeWithNewVid(g1).union(edgeWithNewVid(g2)))
  }

  def main(args: Array[String]): Unit = {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //设置运行环境
    val conf = new SparkConf().setAppName("spark").setMaster("local")
    val sc = new SparkContext(conf)

    val folderPath = "C:\\Users\\hwj\\Documents\\金庸数据\\三部曲（含昵称）\\"

    val graph1 = creatGraph(sc, folderPath + "result1\\heromap.txt",
      folderPath + "result1\\relationship.txt", 15).cache()

    val graph2 = creatGraph(sc, folderPath + "result2\\heromap.txt",
      folderPath + "result2\\relationship.txt", 15).cache()

    val graph3 = creatGraph(sc, folderPath + "result3\\heromap.txt",
      folderPath + "result3\\relationship.txt", 15).cache()

    val graph4 = mergeGraphs(graph1, graph2)
    val graphAll = mergeGraphs(graph3, graph4).cache()

    val minDegreeArray = minDegrees(graphAll)
    //    val graphMin = graphAll.subgraph(vpred = (id,attr)=>minDegreeArray.contains(id),epred = e=>minDegreeArray.contains(e.srcId) || minDegreeArray.contains(e.dstId))
    val graphMin = graphAll.subgraph(epred = e => minDegreeArray.contains(e.srcId) || minDegreeArray.contains(e.dstId))
    //    graphMin.edges.collect().foreach(println)


    // 输出到文件
    val pw1 = new PrintWriter(folderPath + "graph.gexf")
    pw1.write(toGexf(graphAll))
    val pw2 = new PrintWriter(folderPath + "isolate.txt")
    pw2.write(isolate(graphAll))
    val pw3 = new PrintWriter(folderPath + "minDegrees.gexf")
    pw3.write(toGexf(graphMin))

    pw1.close()
    pw2.close()
    pw3.close()
    sc.stop()


  }


}
