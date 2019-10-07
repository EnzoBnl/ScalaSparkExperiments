package org.apache.spark.sql

import com.enzobnl.sparkscalaexpe.playground.GraphxVsGraphFramesPageRanks.{N_ITER, RESET_PROB, execGraphXNoCache, spark}
import com.enzobnl.sparkscalaexpe.playground.Sb3.{DAMPING_FACTOR, N_ITER}
import com.enzobnl.sparkscalaexpe.util.Utils
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.graphframes.GraphFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructField, StructType}
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
        array( // TODO: struct
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
      if ( /*i%3==0 &&*/ i != N_ITER) Utils.time {
        graph.vertices.cache().foreachPartition(_ => {})
      }
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
        array( // TODO: struct
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

  def PR(edges: DataFrame, nIter: Int = N_ITER, processingBatchSize: Int = 5, cutProcessingAndLineage: String = "checkpoint"): Unit = {
    println("Launch PR with ", N_ITER, processingBatchSize, cutProcessingAndLineage)
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

    var cachedDFs: Seq[DataFrame] = Seq[DataFrame]()

    vertices.cache()
    cachedDFs = cachedDFs :+ vertices

    for (i <- 1 to nIter) {
      val enrichedEdges =
        edges.join(
          vertices
            .withColumn("toSend", $"PR" / $"outDegree")
            .select("id", "toSend"),
          $"src" === $"id",
          "inner"
        )
      val summingShipments = enrichedEdges
        .groupBy("dst").sum("toSend")
        .withColumnRenamed("sum(toSend)", "sumSent")

      val updatedVertices =
        summingShipments.join(
          vertices,
          $"dst" === $"id",
          "outer"
        )
          .drop("dst")
          .withColumn("PR", lit(1 - DAMPING_FACTOR) + lit(DAMPING_FACTOR) * expr("ifnull(sumSent, 0)"))
          .drop("sumSent")

      vertices =
        Utils.time {
          cutProcessingAndLineage match {
            case "checkpoint" => {
              if (i % processingBatchSize == 0 && i != nIter) {
                println("PR materializing data, iter n°", i, cutProcessingAndLineage)
                vertices = updatedVertices.cache().localCheckpoint()
                println("PR materialized data, iter n°", i, cutProcessingAndLineage)
                cachedDFs = cachedDFs :+ updatedVertices
                cachedDFs.foreach(_.unpersist())
                cachedDFs = Seq[DataFrame]()
                vertices.cache()
                cachedDFs = cachedDFs :+ vertices
                vertices
              }
              else {
                vertices = updatedVertices
                vertices.cache()
                cachedDFs = cachedDFs :+ vertices
                vertices
              }
            }
            case "toRddAndForEach" => {
              if (i % processingBatchSize == 0 && i != nIter) {
                println("PR materializing data, iter n°", i, cutProcessingAndLineage)
                updatedVertices.cache().foreachPartition(_ => ()) // never uncached the nIter/processingBatchSize updatedVertices DFs
                println("PR materialized data, iter n°", i, cutProcessingAndLineage)
                cachedDFs = cachedDFs :+ vertices
              }
//              vertices.explain(true)
              vertices = DataFrameUtil.createFromInternalRows(updatedVertices.sparkSession, updatedVertices.schema, updatedVertices.queryExecution.toRdd)
//              vertices.explain(true)

              cachedDFs.foreach(_.unpersist())
              cachedDFs = Seq[DataFrame]()
              cachedDFs = cachedDFs :+ vertices
              vertices
            }
          }
        }


    }

    vertices.filter($"id" === 1001866 || $"id" <= 7).show(10)
    cachedDFs.foreach(_.unpersist())
    edges.unpersist()

    println("Close PR with ", N_ITER, processingBatchSize, cutProcessingAndLineage)

  }


  /**
   * Compute PageRank original iterative algorithm
   *  Bench script: scm.Plot(2, "10 iterations of PageRank, 20M links, 6M nodes, RAM >> size (0.7Go), 12 Threads CPU").add(x=["custom DF implem", "GraphX", "GF's AggregateMessages", "GraphFrames"], y=[79000,74445*10/3,196222*10/3,600000*10/3], marker="bar"); plt.show()
   * @param e: first Column represent sources of links and second one represents their
   * @param nIter
   * @param d
   * @return
   */
  def sqlPR(e: DataFrame, nIter: Int = N_ITER, d: Double = DAMPING_FACTOR): (DataFrame, Seq[DataFrame]) = {
    require(e.schema.fields.map(_.name).contains("src"), "e must contain an src col")
    require(e.schema.fields.map(_.name).contains("dst"), "e must contain a dst col")
    require(e.schema(e.schema.getFieldIndex("src").get).dataType == LongType, "src col must be LonType")
    require(e.schema(e.schema.getFieldIndex("dst").get).dataType == LongType, "dst col must be LonType")
    require(nIter > 0, "nIter must be greater than zero")

    // src, dst
    val edges = e.toDF("src", "dst").cache()

    // id
    val ids = edges.selectExpr("src as id")
      .union(edges.selectExpr("dst as id"))
      .distinct()

    // id, outDegree
    val idsAndOutDegrees = ids
      .join(
        edges.groupBy("src").count().withColumnRenamed("count()", "count"),
        ids("id") === edges("src"),
        "outer")
      .withColumn("outDegree", expr("ifnull(count, 0)"))
      .cache()

    // id, PRtoSend
    var vertices = idsAndOutDegrees.withColumn("PRtoSend", lit(1) / col("outDegree"))

    for (_ <- 1 to nIter) {

      // src, dst, PRtoSend
      val weightedEdges = edges.join(vertices, edges("src") === vertices("id"),  "inner").drop("id")

      // dst, PRsumSent
      val summingShipments = weightedEdges
        .groupBy("dst").sum("PRtoSend")
        .withColumnRenamed("sum(PRtoSend)", "PRsumSent")

      // id, PRtoSend
      val updatedVertices = summingShipments
        .join(idsAndOutDegrees,
          summingShipments("dst") === idsAndOutDegrees("id"),
          "outer")
        .withColumn("PRtoSend", (lit(1 - d) + lit(d) * expr("ifnull(PRsumSent, 0)")) / col("outDegree"))
        .select("id", "PRtoSend")

      // cuts DataFrame plan (but not spark lineage like a .checkpoint()
      //      vertices.explain(true)
      //      == Physical Plan ==
      //        Scan ExistingRDD[id#2289,outDegree#2290L,PR#2291]
      vertices = Dataset.ofRows(spark, LogicalRDD(updatedVertices.schema.toAttributes, updatedVertices.queryExecution.toRdd)(spark))
    }
    vertices = vertices.join(idsAndOutDegrees,
      vertices("id") === idsAndOutDegrees("id"),
      "outer")
      .withColumn("PR", col("PRtoSend")*col("outDegree"))
      .select(vertices("id"), col("PR"))
    (vertices, Seq(edges, ids))
  }

  object DataFrameUtil {
    /**
     * Creates a DataFrame out of RDD[InternalRow] that you can get using `df.queryExection.toRdd`
     */
    def createFromInternalRows(sparkSession: SparkSession, schema: StructType, rdd: RDD[InternalRow]): DataFrame = {
      val logicalPlan = LogicalRDD(schema.toAttributes, rdd)(sparkSession)
      Dataset.ofRows(sparkSession, logicalPlan)
    }
  }

  def newAlgo(edgesPath: String, verticesSep: String = ",", verticesPath: Option[String], verticesCols: Seq[String]): Unit = {

    val edges = spark.read
      .format("csv")
      .option("delimiter", " ")
      .schema(StructType(Seq(StructField("src", LongType), StructField("dst", LongType))))
      .load(edgesPath)
      .toDF("src", "dst")
      .cache()


//      Utils.time {
//      PR (edges, N_ITER, 5, "checkpoint")
//    }

//      Utils.time {
//      PR (edges, N_ITER, 5, "toRddAndForEach")
//    }
//    Utils.time {
//      PR(edges, N_ITER, 100, "toRddAndForEach")
//    }
//    Utils.time {
//      sqlPR(edges, N_ITER)
//    }
//    Utils.time {
//      val res = sqlPR(edges, N_ITER)
//      res._1.filter(col("id") === 1001866 || col("id") <= 7).show(10)
//      res._2.foreach(_.unpersist())
//    }
//    println("WAS CUSTOM")

//        Utils.time {
//          pageRankIterJoinsBasedArray(edges).show(10)
//        }

        Utils.time {
          pageRankIterAggregateMessagesArray(edges).show(10)
        }
    println("WAS AggregateMessages")
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
    ranks.filter(id => id._1 == 1001866 || id._1 <= 7).take(100)
  }

  def execGraphFrameNoCache(edgesPath: String, verticesSep: String = ",", verticesPath: Option[String], verticesCols: Seq[String]): Unit = {
    val sc = spark.sparkContext
    // Load the edges as a graph

    val edges = spark.read
      .format("csv")
      .option("delimiter", " ")
      .schema(StructType(Seq(StructField("src", LongType), StructField("dst", LongType))))
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
    for (n_ITER <- Seq(3, 10)) {
      N_ITER = n_ITER
      println("N_ITER=", N_ITER)


//      println(Utils.time {
//        newAlgo(
//          "/home/enzo/Prog/spark/data/graphx/followers.txt",
//          ",",
//          Some("/home/enzo/Prog/spark/data/graphx/users.txt"),
//          Seq("id", "pseudo", "name")
//        )
//      })
//            println(Utils.time {
//              execGraphXNoCache(
//                "/home/enzo/Prog/spark/data/graphx/followers.txt",
//                "\t",
//                Some("/home/enzo/Prog/spark/data/graphx/users.txt")
//              ).toSeq
//            })
//
//            println(Utils.time {
//              execGraphFrameNoCache(
//                "/home/enzo/Prog/spark/data/graphx/followers.txt",
//                ",",
//                Some("/home/enzo/Prog/spark/data/graphx/users.txt"),
//                Seq("id", "pseudo", "name")
//              )
//            })
      println(Utils.time {
        newAlgo(
          "/home/enzo/Prog/spark/data/graphx/followers.txt",
          "\t",
          Some("/home/enzo/Prog/spark/data/graphx/followers.txt"),
          Seq("id", "pseudo")
        )
      })
      println(Utils.time {
        newAlgo(
          "/home/enzo/Data/linkedin.edges",
          "\t",
          Some("/home/enzo/Data/linkedin.nodes"),
          Seq("id", "pseudo")
        )
      })
//            println(Utils.time {
//              execGraphXNoCache(
//                "/home/enzo/Data/linkedin.edges",
//                "\t",
//                Some("/home/enzo/Data/linkedin.nodes")
//              ).toSeq
//            })
//      println("WAS GraphX")
//

            println(Utils.time {
              execGraphFrameNoCache(
                "/home/enzo/Data/linkedin.edges",
                "\t",
                Some("/home/enzo/Data/linkedin.nodes"),
                Seq("id", "pseudo")
              )
            })
      println("WAS GraphFrames")
    }
    while (true) {
      Thread.sleep(1000)
    }
  }


}
