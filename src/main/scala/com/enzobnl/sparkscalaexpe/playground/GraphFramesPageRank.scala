package com.enzobnl.sparkscalaexpe.playground

import com.enzobnl.sparkscalaexpe.playground.GraphxVsGraphFramesPageRanks.{N_ITER, RESET_PROB, execGraphXNoCache, spark}
import com.enzobnl.sparkscalaexpe.playground.Sb3.{DANGLING_FACTOR, N_ITER}
import com.enzobnl.sparkscalaexpe.util.Utils
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.graphframes.GraphFrame
import org.apache.spark.sql.functions._
import org.graphframes.lib.AggregateMessages

object GraphFramesPageRank extends Runnable {
  final val RESET_PROB: Double = 0.15
  var N_ITER: Int = 3
  lazy val spark: SparkSession = SparkSession
    .builder
    .config("spark.default.parallelism", "12")
    .config("spark.default.parallelism", "12")
    .master("local[*]")
    .appName("GraphxVsGraphFramesPageRanksApp")
    .getOrCreate
  spark.sparkContext.setLogLevel("ERROR")

  /**
   * Benches:
   * Mer 18h56: sur linkedin 121119.916117ms (gx: 85434.672529ms)
   * Mer 19h05: sur linkedin 118000.916117ms (gx: 85434.672529ms)
   *
   */
  def pageRankIterAggregateMessagesJoins(vertices: DataFrame, edges: DataFrame, d: Float = 0.85f): GraphFrame = {
    require(0 < d && d < 1, "The damping factor d  must be between 0 and 1 (both excluded)")
    val outDegrees: DataFrame = GraphFrame.fromEdges(edges).outDegrees.withColumn("zero", lit(0)).cache()
    var graph: GraphFrame = GraphFrame(
      vertices
        .withColumn("PR", lit(1))
        .join(outDegrees, Seq("id"))
        .withColumn("PRtoDistribute", col("PR") / col("outDegree"))
      ,
      edges
    )
    println("INIT")
    for (i <- 1 to N_ITER) {
      val am = AggregateMessages
      val agg: DataFrame = {
        graph.aggregateMessages
          .sendToDst(am.src("PRtoDistribute")) // send source user's age to destination
          .sendToSrc(am.src("zero"))
          .agg(sum(am.msg).*(d).+(1 - d).as("PR"))
      } // sum up ages, stored in AM.msg column
      graph = GraphFrame(
        agg
          .join(outDegrees, Seq("id"))
          .withColumn("PRtoDistribute", col("PR") / col("outDegree"))
        ,
        edges
      )
      println("fin ITER n°", i)
      //      graph.vertices.show()
    }
    graph
  }

  def pageRankIterAggregateMessagesArray(e: DataFrame, d: Float = 0.85f): DataFrame = {
    println("PR 2 START")
    require(0 < d && d < 1, "The damping factor d  must be between 0 and 1 (both excluded)")
    val idIndex = 0
    val outDegreeIndex = 1
    val graph1 = GraphFrame.fromEdges(e).cache()

    // id, id_deg_zero
    val outDegrees: DataFrame = graph1
      .outDegrees
      .withColumn(
        "id_deg_zero",
        array(  // TODO: struct
          col("id"),
          col("outDegree")
        ).alias("id_deg_zero"))
      .drop("outDegree")
      .cache()
    //    println("outDegrees")
    //    outDegrees.show()

    // src, dst
    val edges = graph1
      .edges
      .toDF("old_src", "old_dst")
      .join(outDegrees.withColumnRenamed("id", "old_src"), "old_src")
      .withColumnRenamed("id_deg_zero", "src")
      .drop("old_src")
      .join(outDegrees.withColumnRenamed("id", "old_dst"), "old_dst")
      .withColumnRenamed("id_deg_zero", "dst")
      .drop("old_dst")
      .cache()
    //    println("edges")
    //    edges.show()
    graph1.unpersist()
    outDegrees.unpersist()
    // id
    val vertices = outDegrees
      .drop("id")
      .withColumnRenamed("id_deg_zero", "id")
    //    println("vertices")
    //    vertices.show()

    var graph: GraphFrame = GraphFrame(
      vertices
        .withColumn("PR", lit(1))
        .withColumn("PRtoDistribute", col("PR") / col("id")(outDegreeIndex))
      ,
      edges
    )
    //    println("graph.vertices")
    //    graph.vertices.show()
    //    println("graph.edges")
    //    graph.edges.show()

    println("INIT")
    val am = AggregateMessages
    for (i <- 1 to N_ITER) {
      val agg: DataFrame = {
        graph.aggregateMessages
          .sendToDst(am.src("PRtoDistribute")) // send source user's age to destination
          .sendToSrc(lit(0))
          .agg(sum(am.msg).*(d).+(1 - d).as("PR"))
      }
      graph.vertices.unpersist()
      graph = GraphFrame(
        agg
          .withColumn(
            "PRtoDistribute",
            col("PR") / col("id")(outDegreeIndex)
          )
        ,
        edges
      )
      println("deb ITER n°", i)
      if(/*i%3==0 &&*/ i!=N_ITER)Utils.time {graph.vertices.repartition(12).foreachPartition(_ => {})}
      println("fin ITER n°", i)
    }
    println("PR 2 END")
    graph.vertices.select(col("id"), col("PR"))
  }

  def pageRankIterJoinsBasedArray(e: DataFrame, d: Float = 0.85f): DataFrame = {
    println("PR 2 START")
    require(0 < d && d < 1, "The damping factor d  must be between 0 and 1 (both excluded)")
    val idIndex = 0
    val outDegreeIndex = 1
    val graph1 = GraphFrame.fromEdges(e).cache()

    // id, id_deg_zero
    val outDegrees: DataFrame = graph1
      .outDegrees.repartition(12)
      .withColumn(
        "id_deg_zero",
        array(  // TODO: struct
          col("id"),
          col("outDegree")
        ).alias("id_deg_zero"))
      .drop("outDegree")
      .cache()
    //    println("outDegrees")
    //    outDegrees.show()

    // src, dst
    val edges = graph1
      .edges.repartition(12)
      .toDF("old_src", "old_dst")
      .join(outDegrees.withColumnRenamed("id", "old_src"), "old_src")
      .withColumnRenamed("id_deg_zero", "src")
      .drop("old_src")
      .join(outDegrees.withColumnRenamed("id", "old_dst"), "old_dst")
      .withColumnRenamed("id_deg_zero", "dst")
      .drop("old_dst")
      .repartition(12)
      .cache()
    //    println("edges")
    //    edges.show()
    graph1.unpersist()
    outDegrees.unpersist()
    // id
    val vertices = outDegrees
      .drop("id")
      .withColumnRenamed("id_deg_zero", "id")
    //    println("vertices")
    //    vertices.show()

    var graph: GraphFrame = GraphFrame(
      vertices
        .withColumn("PR", lit(1))
        .withColumn("PRtoDistribute", col("PR") / col("id")(outDegreeIndex))
      ,
      edges
    )
    //    println("graph.vertices")
    //    graph.vertices.show()
    //    println("graph.edges")
    //    graph.edges.show()

    println("INIT")
    val am = AggregateMessages
    for (i <- 1 to N_ITER) {
      val agg: DataFrame = graph
        .aggregateMessages
        .sendToDst(am.src("PRtoDistribute")) // send source user's age to destination
        .sendToSrc(lit(0))
        .agg(sum(am.msg).*(d).+(1 - d).as("PR"))
        .repartition(12)

//      println(graph.edges.rdd.partitions.length)
//      println(agg.rdd.partitions.length)

      graph = GraphFrame(
        agg
          .withColumn(
            "PRtoDistribute",
            col("PR") / col("id")(outDegreeIndex)
          )
        ,
        edges
      )
//      println(graph.vertices.rdd.partitions.length)

      println("deb ITER n°", i)
//      if(/*i%3==0 &&*/ i!=N_ITER)Utils.time {graph.vertices.repartition(12).foreachPartition(_ => {})}
      println("fin ITER n°", i)
    }
    println("PR 2 END")
    graph.vertices.select(col("id"), col("PR"))
  }
  def PR(edges: DataFrame, nIter: Int = N_ITER, verbose: Boolean = false): Unit = {
    import spark.implicits._
    edges.cache()
    val ids = edges
      .select(col("src").as("id"))
      .union(edges.select(col("dst").as("id")))
      .distinct()

    var vertices = ids.join(
      edges.groupBy("src").count(),
      $"src" === $"id",
      "outer"
    )
      .drop("src")
      .withColumnRenamed("count()", "count")
      .withColumn("outDegree", expr("ifnull(count, 0)"))
      .drop("count")
      .withColumn("PR", lit(1))
      .cache()

    for (i <- 1 to nIter) {
      //          edges.as("a").join(
      val enrichedEdges =
        edges.as("c").join(
          vertices
            .withColumn("toSend", $"PR" / $"outDegree")
            .select("id", "toSend").as("d"),
          $"c.src" === $"d.id",
          "inner"
        )
      if (verbose) enrichedEdges.show()

      val summingShipments = enrichedEdges
        .groupBy("dst").sum("toSend")
        .withColumnRenamed("sum(toSend)", "sumSent")
      if (verbose) summingShipments.show()

      vertices =
        summingShipments.as("c").join(
          vertices.as("d"),
          $"c.dst" === $"d.id",
          "outer"
        )
          .drop("dst")
          .withColumn("PR", lit(1 - DANGLING_FACTOR) + lit(DANGLING_FACTOR) * expr("ifnull(sumSent, 0)"))
          .drop("sumSent").cache()
      if (verbose) vertices.show()
    }

    vertices.filter($"id" === 1001866 || $"id" <= 7).show(10)
  }
  def newAlgo(edgesPath: String, verticesSep: String = ",", verticesPath: Option[String], verticesCols: Seq[String]): Unit = {

    val edges = spark.read
      .format("csv")
      .option("delimiter", " ")
      .load(edgesPath)
      .toDF("src", "dst")
      .cache()




    Utils.time {
      PR(edges)
    }

//    Utils.time {
//      pageRankIterJoinsBasedArray(edges).show(10)
//    }

//    Utils.time {
//      pageRankIterAggregateMessagesArray(edges).show(10)
//    }
//val vertices = spark.read
//  .format("csv")
//  .option("delimiter", verticesSep)
//  .load(verticesPath.get)
//  .toDF(verticesCols: _*)
//  .withColumn("PR", lit(1))
//  .cache()
//    Utils.time {
//      pageRankIterAggregateMessagesJoins(vertices, edges)
//        .vertices
//        .show(10)
//    }
  }

  def execGraphXNoCache(edgesPath: String, verticesSep: String = ",", verticesPath: Option[String]): Array[(Long, Double)] = {
    val sc = spark.sparkContext
    val graph = GraphLoader.edgeListFile(sc, edgesPath)
    val ranks = graph.staticPageRank(N_ITER, RESET_PROB).vertices
    ranks.filter(id => id._1 == 1001866 || id._1 <=7).take(100)
  }

  def execGraphFrameNoCache(edgesPath: String, verticesSep: String = ",", verticesPath: Option[String], verticesCols: Seq[String]): Unit = {
    val sc = spark.sparkContext
    // Load the edges as a graph

    val edges = spark.read
      .format("csv")
      .option("delimiter", " ")
      .load(edgesPath)
      .toDF("src", "dst")

    val graph: GraphFrame = GraphFrame.fromEdges(edges)
    val pageRankResult: GraphFrame = graph.pageRank.resetProbability(RESET_PROB).maxIter(N_ITER).run()
    import spark.implicits._
    pageRankResult
      .vertices
      //      .join(vertices, Seq("id", "id"))
      //      .select("pseudo", "pageRank")
      //      .as[(String, Double)]
      .show(10)
  }

  override def run(): Unit = {
    for (n_ITER <- Seq(3, 6, 12, 24, 48)) {
      N_ITER = n_ITER
      println(Utils.time {
        newAlgo(
          "/home/enzo/Prog/spark/data/graphx/followers.txt",
          ",",
          Some("/home/enzo/Prog/spark/data/graphx/users.txt"),
          Seq("id", "pseudo", "name")
        )
      })
      println(Utils.time {
        execGraphXNoCache(
          "/home/enzo/Prog/spark/data/graphx/followers.txt",
          "\t",
          Some("/home/enzo/Prog/spark/data/graphx/users.txt")
        ).toSeq
      })

//      println(Utils.time {
//        execGraphFrameNoCache(
//          "/home/enzo/Prog/spark/data/graphx/followers.txt",
//          ",",
//          Some("/home/enzo/Prog/spark/data/graphx/users.txt"),
//          Seq("id", "pseudo", "name")
//        )
//      })

      println(Utils.time {
        newAlgo(
          "/home/enzo/Data/linkedin.edges",
          "\t",
          Some("/home/enzo/Data/linkedin.nodes"),
          Seq("id", "pseudo")
        )
      })
      println(Utils.time {
        execGraphXNoCache(
          "/home/enzo/Data/linkedin.edges",
          "\t",
          Some("/home/enzo/Data/linkedin.nodes")
        ).toSeq
      })



//      println(Utils.time {
//        execGraphFrameNoCache(
//          "/home/enzo/Data/linkedin.edges",
//          "\t",
//          Some("/home/enzo/Data/linkedin.nodes"),
//          Seq("id", "pseudo")
//        )
//      })
    }
    while (true) {
      Thread.sleep(1000)
    }
  }


}
