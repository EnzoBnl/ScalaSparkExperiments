package com.enzobnl.sparkscalaexpe.playground

import java.util

import com.enzobnl.sparkscalaexpe.util.QuickSparkSessionFactory
import org.apache.spark.ml.feature.SQLTransformer
import org.apache.spark.graphx.GraphLoader

import scala.util.Random
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType



object GraphxVsGraphFrames extends Runnable {

  lazy val spark: SparkSession = QuickSparkSessionFactory.getOrCreate()

  override def run(): Unit = {
    val sc = spark.sparkContext
    // Load the edges as a graph
    val sparkPath = "/home/enzo/Prog/spark/"
    val graph = GraphLoader.edgeListFile(sc, sparkPath + "data/graphx/followers.txt")
    // Run PageRank
    val ranks = graph.pageRank(0.0001).vertices
    // Join the ranks with the usernames
    val users = sc.textFile(sparkPath + "data/graphx/users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val ranksByUsername = users.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }
    // Print the result
    println(ranksByUsername.collect().mkString("\n"))
  }
}
