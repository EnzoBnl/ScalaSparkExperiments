package com.enzobnl.sparkscalaexpe.playground

import com.enzobnl.sparkscalaexpe.util.Utils
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame


object GraphxVsGraphFramesPageRanks extends Runnable {
  final val RESET_PROB: Double = 0.15
  final val N_ITER: Int = 3
  lazy val spark: SparkSession = SparkSession
    .builder
    .config("spark.default.parallelism", "12")
    .master("local[*]")
    .appName("GraphxVsGraphFramesPageRanksApp")
    .getOrCreate
  spark.sparkContext.setLogLevel("ERROR")

  def execGraphX(edgesPath: String, verticesSep: String = ",", verticesPath: Option[String]): Array[(String, Double)] = {
    val sc = spark.sparkContext
    // Load the edges as a graph
    val graph = GraphLoader.edgeListFile(sc, edgesPath)
    graph.cache()
    println("N° edges =" + graph.edges.count())
    println("N° vertices =" + graph.vertices.count())

    // Run PageRank
    val ranks = graph.staticPageRank(N_ITER, RESET_PROB).vertices
    // Join the ranks with the usernames
    val users = sc.textFile(verticesPath.get)
      .map { line => line.split(verticesSep) }
      .filter { fields => fields.size >= 2 && fields(0) != "" }
      .map { fields => (fields(0).toLong, fields(1)) }
    val ranksByUsername = users.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }
    // Print the result
    ranksByUsername.take(100)
  }
  def execGraphXNoCache(edgesPath: String, verticesSep: String = ",", verticesPath: Option[String]): Array[(String, Double)] = {
    val sc = spark.sparkContext
    // Load the edges as a graph
    val graph = GraphLoader.edgeListFile(sc, edgesPath)
    graph
    println("N° edges =" + graph.edges.count())
    println("N° vertices =" + graph.vertices.count())

    // Run PageRank
    val ranks = graph.staticPageRank(N_ITER, RESET_PROB).vertices
    // Join the ranks with the usernames
    val users = sc.textFile(verticesPath.get)
      .map { line => line.split(verticesSep) }
      .filter { fields => fields.size >= 2 && fields(0) != "" }
      .map { fields => (fields(0).toLong, fields(1)) }
    val ranksByUsername = users.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }
    // Print the result
    ranksByUsername.take(100)
  }

  def execGraphFrame(edgesPath: String, verticesSep: String = ",", verticesPath: Option[String], verticesCols: Seq[String]): Array[(String, Double)] = {
    val sc = spark.sparkContext
    // Load the edges as a graph

    val edges = spark.read
      .format("csv")
      .option("delimiter", " ")
      .load(edgesPath)
      .toDF("src", "dst").cache()
    println("N° edges =" + edges.count())
    val vertices = spark.read
      .format("csv")
      .option("delimiter", verticesSep)
      .load(verticesPath.get)
      .toDF(verticesCols: _*).cache()
    println("N° vertices =" + vertices.count())
    val graph: GraphFrame = GraphFrame.fromEdges(edges)
    val pageRankResult: GraphFrame = graph.pageRank.resetProbability(RESET_PROB).maxIter(N_ITER).run()
    import spark.implicits._
    pageRankResult
      .vertices
      .join(vertices, Seq("id", "id"))
      .select("pseudo", "pageRank")
      .as[(String, Double)].limit(100)
      .collect()
  }

  def execGraphFrameNoCache(edgesPath: String, verticesSep: String = ",", verticesPath: Option[String], verticesCols: Seq[String]): Array[(String, Double)] = {
    val sc = spark.sparkContext
    // Load the edges as a graph

    val edges = spark.read
      .format("csv")
      .option("delimiter", " ")
      .load(edgesPath)
      .toDF("src", "dst")
    println("N° edges =" + edges.count())
    val vertices = spark.read
      .format("csv")
      .option("delimiter", verticesSep)
      .load(verticesPath.get)
      .toDF(verticesCols: _*)
    println("N° vertices =" + vertices.count())
    val graph: GraphFrame = GraphFrame.fromEdges(edges)
    val pageRankResult: GraphFrame = graph.pageRank.resetProbability(RESET_PROB).maxIter(N_ITER).run()
    import spark.implicits._
    pageRankResult
      .vertices
      .join(vertices, Seq("id", "id"))
      .select("pseudo", "pageRank")
      .as[(String, Double)].limit(100)
      .collect()
  }
  override def run(): Unit = {
    val gx: Seq[(String, Double)] = Utils.time {
      execGraphX(
        "/home/enzo/Prog/spark/data/graphx/followers.txt",
        ",",
        Some("/home/enzo/Prog/spark/data/graphx/users.txt")
      ).toSeq
    }
    val gf: Seq[(String, Double)] = Utils.time {
      execGraphFrame(
        "/home/enzo/Prog/spark/data/graphx/followers.txt",
        ",",
        Some("/home/enzo/Prog/spark/data/graphx/users.txt"),
        Seq("id", "pseudo", "name")
      ).toSeq
    }
    //    while(true){
    //      Thread.sleep(1000)
    //    }
    val gx2: Seq[(String, Double)] = Utils.time {
      execGraphX(
        "/home/enzo/Data/linkedin.edges",
        "\t",
        Some("/home/enzo/Data/linkedin.nodes")
      ).toSeq
    }
    val gf2: Seq[(String, Double)] = Utils.time {
      execGraphFrame(
        "/home/enzo/Data/linkedin.edges",
        "\t",
        Some("/home/enzo/Data/linkedin.nodes"),
        Seq("id", "pseudo")
      ).toSeq
    }
    val gx2n: Seq[(String, Double)] = Utils.time {
      execGraphXNoCache(
        "/home/enzo/Data/linkedin.edges",
        "\t",
        Some("/home/enzo/Data/linkedin.nodes")
      ).toSeq
    }
    val gf2n: Seq[(String, Double)] = Utils.time {
      execGraphFrameNoCache(
        "/home/enzo/Data/linkedin.edges",
        "\t",
        Some("/home/enzo/Data/linkedin.nodes"),
        Seq("id", "pseudo")
      ).toSeq
    }
    println(gx)
    println(gf)
    println(gx2)
    println(gf2)
    println(gx2n)
    println(gf2n)
    while (true) {
      Thread.sleep(1000)
    }
  }


}
