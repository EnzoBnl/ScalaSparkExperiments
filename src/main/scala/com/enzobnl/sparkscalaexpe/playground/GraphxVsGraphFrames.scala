package com.enzobnl.sparkscalaexpe.playground

import java.util

import com.enzobnl.sparkscalaexpe.util.{QuickSparkSessionFactory, Utils}
import org.apache.spark.ml.feature.SQLTransformer
import org.apache.spark.graphx.GraphLoader

import scala.util.Random
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.graphframes.GraphFrame


object GraphxVsGraphFrames extends Runnable {
  final val RESET_PROB: Double = 0.15
  final val N_ITER: Int = 10
  lazy val spark: SparkSession = QuickSparkSessionFactory.getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  def execGraphX(): Array[(String, Double)] = {
    val sc = spark.sparkContext
    // Load the edges as a graph
    val sparkPath = "/home/enzo/Prog/spark/"
    val graph = GraphLoader.edgeListFile(sc, sparkPath + "data/graphx/followers.txt")
    // Run PageRank
    val ranks = graph.staticPageRank(N_ITER, RESET_PROB).vertices
    // Join the ranks with the usernames
    val users = sc.textFile(sparkPath + "data/graphx/users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val ranksByUsername = users.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }
    // Print the result
    ranksByUsername.collect()
  }

  def execGraphFrame(): Array[(String, Double)] = {
    val sc = spark.sparkContext
    // Load the edges as a graph
    val sparkPath = "/home/enzo/Prog/spark/"
    val edges = spark.read
      .format("csv")
      .option("delimiter", " ")
      .load(sparkPath + "data/graphx/followers.txt")
      .toDF("src", "dst")
    val vertices = spark.read
      .format("csv")
      .option("delimiter", ",")
      .load(sparkPath + "data/graphx/users.txt")
      .toDF("id", "pseudo", "name")

    val graph: GraphFrame = GraphFrame.fromEdges(edges)
    val pageRankResult: GraphFrame = graph.pageRank.resetProbability(RESET_PROB).maxIter(N_ITER).run()
    import spark.implicits._
    pageRankResult
      .vertices
      .join(vertices, Seq("id", "id"))
      .select("pseudo", "pageRank")
      .as[(String, Double)]
      .collect()
  }

  override def run(): Unit = {

    val gx: Seq[(String, Double)] = Utils.time {execGraphX().toSeq}
    val gf: Seq[(String, Double)] = Utils.time {execGraphFrame().toSeq}
    println(gx)
    println(gf)
  }



}
