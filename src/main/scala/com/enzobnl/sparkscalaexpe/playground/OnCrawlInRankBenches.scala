package com.enzobnl.sparkscalaexpe.playground

import com.enzobnl.sparkscalaexpe.playground.Sb3.DAMPING_FACTOR
import com.enzobnl.sparkscalaexpe.util.Utils
import org.apache.spark.{HashPartitioner, Partition}
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkUtils.withNumShufflePartition
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, SparkUtils}
import org.graphframes.GraphFrame
import org.graphframes.lib.AggregateMessages


object OnCrawlInRankBenches extends Runnable {
  final val RESET_PROB: Float = 0.15f
  var N_ITER: Int = 0
  lazy val spark: SparkSession = SparkSession
    .builder
    .config("spark.default.parallelism", (Runtime.getRuntime().availableProcessors() * 4).toString)
    .config("spark.sql.shuffle.partitions", Runtime.getRuntime().availableProcessors() * 4)
    .master("local[*]")
    .appName("OnCrawlInRankBenches")
    .getOrCreate
  spark.sparkContext.setLogLevel("ERROR")

  //
  // ALGORITHMS
  //
  def PRGraphXBuiltIn(edgesPath: String): Unit = {
    val sc = spark.sparkContext
    val graph = GraphLoader.edgeListFile(sc, edgesPath)
    val ranks = graph.staticPageRank(N_ITER, RESET_PROB).vertices
    println(ranks.filter(id => id._1 == 1072133284 || (id._1 >= 0 && id._1 <= 7)).take(10).toSeq)
  }

  def PRSoundCloud(edgesPath: String, verticesSep: String = ",", verticesPath: Option[String]): Unit = {
    import spark.implicits._
    val edgesRDD = spark.read
      .format("csv")
      .option("delimiter", " ")
      .schema(StructType(Seq(StructField("src", LongType), StructField("dst", LongType))))
      .load(edgesPath)
      .toDF("src", "dst")
      .map(row => com.soundcloud.spark.pagerank.Edge(row.getAs(0), row.getAs(0), 1))
      .rdd
    spark.sparkContext.setCheckpointDir("./checkpoint")
    val g = com.soundcloud.spark.pagerank.PageRankGraph.fromEdgesWithUniformPriors(edgesRDD, StorageLevels.MEMORY_ONLY, StorageLevels.MEMORY_AND_DISK, StorageLevels.MEMORY_AND_DISK)
    println(com.soundcloud.spark.pagerank.PageRank.run(g, 0.15, N_ITER)
      .filter(id => id.id == 1072133284 || (id.id >= 0 && id.id <= 7)).take(10).toSeq)
  }

  def PRGraphFramesBuiltIn(edgesPath: String, verticesSep: String = ",", verticesPath: Option[String], verticesCols: Seq[String]): Unit = {

    val edges = spark.read
      .format("csv")
      .option("delimiter", " ")
      .schema(StructType(Seq(StructField("src", LongType), StructField("dst", LongType))))
      .load(edgesPath)
      .toDF("src", "dst").cache()

    val graph: GraphFrame = GraphFrame.fromEdges(edges)
    val pageRankResult: GraphFrame = graph.pageRank.resetProbability(RESET_PROB).maxIter(N_ITER).run()

    pageRankResult.vertices.filter(col("id") === 1072133284 || (col("id") >= 0 && col("id") <= 7)).show(10)
  }

  /**
   * Benches:
   * Mer 18h56: sur linkedin 121119.916117ms (gx: 85434.672529ms)
   * Mer 19h05: sur linkedin 118000.916117ms (gx: 85434.672529ms)
   *
   */
  def AggMess(edges: DataFrame, d: Float = (1 - RESET_PROB)): Unit = {
    require(0 < d && d < 1, "The damping factor d  must be between 0 and 1 (both excluded)")
    edges.cache()
    val outDegrees: DataFrame = GraphFrame.fromEdges(edges).outDegrees.withColumn("zero", lit(0)).cache()
    var graph: GraphFrame = GraphFrame(
      edges.selectExpr("src as id")
        .union(edges.selectExpr("dst as id"))
        .distinct()
        .withColumn("PR", lit(1))
        .join(outDegrees, Seq("id"))
        .withColumn("PRtoDistribute", col("PR") / col("outDegree"))
      ,
      edges
    )
    //    println("INIT")
    for (i <- 1 to N_ITER) {
      val am = AggregateMessages
      val agg: DataFrame = {
        graph.aggregateMessages
          .sendToDst(am.src("PRtoDistribute")) // send source user's age to destination
          .sendToSrc(am.src("zero"))
          .agg(sum(am.msg).*(d).+(1 - d).as("PR"))
      } // sum up ages, stored in AM.msg column
      val newVertices = agg
        .join(outDegrees, Seq("id"))
        .withColumn("PRtoDistribute", col("PR") / col("outDegree"))
      graph = GraphFrame(
        SparkUtils.DatasetofRows(spark, newVertices.schema, newVertices.queryExecution.toRdd)
        ,
        edges
      )
      //      println("fin ITER n°", i)
    }
    graph.vertices.filter(col("id") === 1072133284 || (col("id") >= 0 && col("id") <= 7)).show(10)
    edges.unpersist(false)
  }

  def PRAggMessJoinFree(e: DataFrame, d: Float = 0.85f): DataFrame = {
    println("PR 2 START")
    require(0 < d && d < 1, "The damping factor d  must be between 0 and 1 (both excluded)")
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

    graph1.unpersist(false)
    outDegrees.unpersist(false)
    // id
    val vertices = outDegrees
      .drop("id")
      .withColumnRenamed("id_deg_zero", "id")

    var graph: GraphFrame = GraphFrame(
      vertices
        .withColumn("PR", lit(1))
        .withColumn("PRtoDistribute", col("PR") / col("id")(outDegreeIndex))
      ,
      edges
    )

    val am = AggregateMessages
    for (i <- 1 to N_ITER) {
      val agg: DataFrame = {
        graph.aggregateMessages
          .sendToDst(am.src("PRtoDistribute")) // send source user's age to destination
          .sendToSrc(lit(0))
          .agg(sum(am.msg).*(d).+(1 - d).as("PR"))
      }
      graph.vertices.unpersist(false)
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
      if (i != N_ITER) Utils.time {
        graph.vertices.cache().foreachPartition(_ => {}) // TODO: fucked up
      }
      println("fin ITER n°", i)
    }
    println("PR 2 END")
    graph.vertices.select(col("id"), col("PR"))
  }

  /**
   * Compute PageRank original iterative algorithm
   * Bench script: scm.Plot(2, "10 iterations of PageRank, 20M links, 6M nodes, RAM >> size (0.7Go), 12 Threads CPU").add(x=["custom DF implem", "GraphX", "GF's AggregateMessages", "GraphFrames"], y=[79000,74445*10/3,196222*10/3,600000*10/3], marker="bar"); plt.show()
   * TODO: add .cache().checkpoint(eager=false) each 10 iteration to face retries more
   *
   * @param e : first Column represent sources of links and second one represents their
   * @param nIter
   * @param d
   * @return
   */
  def PRpureSQL(e: DataFrame, nIter: Int = N_ITER, d: Double = DAMPING_FACTOR): (DataFrame, Seq[DataFrame]) = {
    require(e.schema.fields(0).name == "src", "e must contain an src col at pos 0")
    require(e.schema.fields(1).name == "dst", "e must contain a dst col at pos 1")
    require(e.schema.fields(0).dataType == LongType, "src col must be LongType")
    require(e.schema.fields(1).dataType == LongType, "dst col must be LongType")
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

    for (i <- 1 to nIter) {

      // src, dst, PRtoSend
      val weightedEdges = edges.join(vertices, edges("src") === vertices("id"), "inner").drop("id")

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
      if (i != nIter) {
        // cuts DataFrame plan (but not spark lineage like a .checkpoint()
        //      vertices.explain(true)
        //      == Physical Plan ==
        //        Scan ExistingRDD[id#2289,outDegree#2290L,PR#2291]
        vertices = SparkUtils.DatasetofRows(spark, updatedVertices.schema, updatedVertices.queryExecution.toRdd)
      }
      else {
        vertices = updatedVertices
      }
    }
    vertices = vertices.join(idsAndOutDegrees,
      vertices("id") === idsAndOutDegrees("id"),
      "outer")
      .withColumn("PR", col("PRtoSend") * col("outDegree"))
      .select(vertices("id"), col("PR"))

    (vertices, Seq(edges, ids))
  }

  def PRpureSQLpart(e: DataFrame, nIter: Int = N_ITER, d: Double = DAMPING_FACTOR): (DataFrame, Seq[DataFrame]) = {
    require(e.schema.fields(0).name == "src", "e must contain an src col at pos 0")
    require(e.schema.fields(1).name == "dst", "e must contain a dst col at pos 1")
    require(e.schema.fields(0).dataType == LongType, "src col must be LongType")
    require(e.schema.fields(1).dataType == LongType, "dst col must be LongType")
    require(nIter > 0, "nIter must be greater than zero")

    // src, dst
    val edgesSRCpart = e.toDF("src", "dst").repartition(col("src")).cache()

    // id
    val ids = edgesSRCpart.selectExpr("src as id")
      .union(edgesSRCpart.selectExpr("dst as id"))
      .distinct()

    // id, outDegree
    val idsAndOutDegrees = ids
      .join(
        edgesSRCpart.groupBy("src").count().withColumnRenamed("count()", "count"),
        ids("id") === edgesSRCpart("src"),
        "outer")
      .withColumn("outDegree", expr("ifnull(count, 0)"))
      .repartition(col("id"))
      .cache()

    // id, PRtoSend
    var vertices = idsAndOutDegrees.withColumn("PRtoSend", lit(1) / col("outDegree"))

    for (i <- 1 to nIter) {

      // src, dst, PRtoSend
      val weightedEdges = edgesSRCpart.join(vertices, edgesSRCpart("src") === vertices("id"), "inner").drop("id")

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
      if (i != nIter) {
        // cuts DataFrame plan (but not spark lineage like a .checkpoint()
        //      vertices.explain(true)
        //      == Physical Plan ==
        //        Scan ExistingRDD[id#2289,outDegree#2290L,PR#2291]
        vertices = SparkUtils.DatasetofRows(spark, updatedVertices.schema, updatedVertices.queryExecution.toRdd)
      }
      else {
        vertices = updatedVertices
      }
    }
    vertices = vertices.join(idsAndOutDegrees,
      vertices("id") === idsAndOutDegrees("id"),
      "outer")
      .withColumn("PR", col("PRtoSend") * col("outDegree"))
      .select(vertices("id"), col("PR"))

    (vertices, Seq(edgesSRCpart, ids))
  }

  /**
   * Compute PageRank original iterative algorithm
   * Bench script: scm.Plot(2, "10 iterations of PageRank, 20M links, 6M nodes, RAM >> size (0.7Go), 12 Threads CPU").add(x=["custom DF implem", "GraphX", "GF's AggregateMessages", "GraphFrames"], y=[79000,74445*10/3,196222*10/3,600000*10/3], marker="bar"); plt.show()
   *
   * @param e : first Column represent sources of links and second one represents their
   * @param nIter
   * @param d
   * @return
   */
  def PRpureSQLOnlyWithEdges(e: DataFrame, nIter: Int = N_ITER, d: Double = DAMPING_FACTOR): (DataFrame, Seq[DataFrame]) = {
    require(e.schema.fields(0).name == "src", "e must contain an src col at pos 0")
    require(e.schema.fields(1).name == "dst", "e must contain a dst col at pos 1")
    require(e.schema.fields(0).dataType == LongType, "src col must be LongType")
    require(e.schema.fields(1).dataType == LongType, "dst col must be LongType")
    require(nIter > 0, "nIter must be greater than zero")

    // src, dst
    //    spark.createDataFrame(Seq((1L,2L),(2L,3L), (2L,1L))).toDF("src", "dst")
    val edges = e.toDF("src", "dst").cache()

    // src, dst
    val completedEdges = edges.selectExpr("dst as id").as("a")
      .join(edges.selectExpr("src as id").as("b"), col("a.id") === col("b.id"), "leftanti")
      .withColumnRenamed("id", "src")
      .withColumn("dst", lit(-1))
      .union(edges).cache()
    // src, dst, outDegree, PRtoSend
    var edgesAndOutDegreesAndPRtoSend = completedEdges.as("a")
      .join(
        completedEdges.groupBy("src").count().withColumnRenamed("count()", "count").as("b"),
        col("a.src") === col("b.src"),
        "outer")
      .drop(col("a.src"))
      .withColumn("outDegree", expr("ifnull(count, 0)"))
      .withColumn("PRtoSend", lit(1) / col("outDegree")).drop("count")
      .cache

    var toUnpersist = Seq(edges, completedEdges, edgesAndOutDegreesAndPRtoSend)
    for (i <- 1 to nIter) {

      val summingShipments = edgesAndOutDegreesAndPRtoSend
        .filter(col("dst") !== lit(-1L))
        .groupBy("dst").sum("PRtoSend")
        .toDF("dst", "PRsumSent")

      val updatedEdgesAndOutDegreesAndPRtoSend = summingShipments
        .join(edgesAndOutDegreesAndPRtoSend,
          summingShipments("dst") === edgesAndOutDegreesAndPRtoSend("src"),
          "outer")
        .withColumn("PRtoSend", (lit(1 - d) + lit(d) * expr("ifnull(PRsumSent, 0)")) / col("outDegree"))
        .drop("PRsumSent")
        .drop(summingShipments("dst"))

      //      updatedEdgesAndOutDegreesAndPRtoSend.show()
      if (i == nIter) {
        edgesAndOutDegreesAndPRtoSend = edgesAndOutDegreesAndPRtoSend.withColumn("PR", col("PRtoSend") * col("outDegree"))
          .select(edgesAndOutDegreesAndPRtoSend("src").as("id"), col("PR")).distinct()
      }
      else {
        edgesAndOutDegreesAndPRtoSend = SparkUtils.DatasetofRows(spark, updatedEdgesAndOutDegreesAndPRtoSend.schema, updatedEdgesAndOutDegreesAndPRtoSend.queryExecution.toRdd).cache()
        toUnpersist = toUnpersist :+ edgesAndOutDegreesAndPRtoSend
      }
    }

    (edgesAndOutDegreesAndPRtoSend, toUnpersist)
  }

  //
  // Benches
  //
  def benchsqlPRGraphXGraphFrames() = {
    for (nIter <- Seq(1, 2, 3, 10)) {
      this.N_ITER = nIter
      print("BENCH ITER MAX", this.N_ITER)

      Utils.time {
        val res = PRpureSQL(spark.read
          .format("csv")
          .option("delimiter", " ")
          .schema(StructType(Seq(StructField("src", LongType), StructField("dst", LongType))))
          .load("/home/enzo/Prog/spark/data/graphx/followers.txt")
          .toDF("src", "dst")
          .cache(), N_ITER)
        res._1.filter(col("id") === 1072133284 || (col("id") >= 0 && col("id") <= 7)).show(10)
        res._2.foreach(_.unpersist(false))
      }

      Utils.time {
        val res = PRpureSQL(spark.read
          .format("csv")
          .option("delimiter", " ")
          .schema(StructType(Seq(StructField("src", LongType), StructField("dst", LongType))))
          .load("/home/enzo/Data/linkedin.edges")
          .toDF("src", "dst")
          .cache(), N_ITER)
        res._1.filter(col("id") === 1072133284 || (col("id") >= 0 && col("id") <= 7)).show(10)
        res._2.foreach(_.unpersist(false))
      }
      println("WAS CUSTOM")

      Utils.time {
        PRGraphXBuiltIn(
          "/home/enzo/Data/linkedin.edges")
      }
      println("WAS GraphX")
      //

      Utils.time {
        PRGraphFramesBuiltIn(
          "/home/enzo/Data/linkedin.edges",
          "\t",
          Some("/home/enzo/Data/linkedin.nodes"),
          Seq("id", "pseudo")
        )
      }
      println("WAS GraphFrames")
    }
  }

  /**
   * p = scm.Plot(ylabel="running time (s)", xlabel="n°Iter", title="6M7 nodes, 19M3 edges, 600Mo, on 12 threads CPU, everything fitting in RAM", borders=[0, 20, 0, 250]); p.add(x=[1, 2, 3, 5, 8, 12, 20], y=[14.2, 12.8, 17.4, 27.9, 49.6, 71.8, 126.7], marker="-", label="custom SparkSQL implem"); p.add(x=[1, 2, 3, 5, 8, 12, 20], y=[19.8, 20.1, 21.3, 26.4, 32.8, 41.8, 61], marker="-", label="GraphX built-in");p.add(x=[1, 2, 3, 5, 8, 12], y=[62, 61, 80.5, 115.5, 174, 245], marker="-", label="SoundCloud' RDD-based implem"); plt.show()
   */
  def benchsqlPRGraphXSoundCloud() = {
    for (nIter <- Seq(1, 2, 3, 5, 8, 12, 20)) {
      this.N_ITER = nIter
      print("BENCH ITER MAX", this.N_ITER)

      Utils.time {
        val res = PRpureSQL(spark.read
          .format("csv")
          .option("delimiter", " ")
          .schema(StructType(Seq(StructField("src", LongType), StructField("dst", LongType))))
          .load("/home/enzo/Prog/spark/data/graphx/followers.txt")
          .toDF("src", "dst")
          .cache(), N_ITER)
        res._1.filter(col("id") === 1072133284 || (col("id") >= 0 && col("id") <= 7)).show(10)
        res._2.foreach(_.unpersist(false))
      }

      Utils.time {
        PRSoundCloud(
          "/home/enzo/Data/linkedin.edges",
          "\t",
          Some("/home/enzo/Data/linkedin.nodes")
        )
      }
      println("WAS Soundcloud")
      Utils.time {
        val res = PRpureSQL(spark.read
          .format("csv")
          .option("delimiter", " ")
          .schema(StructType(Seq(StructField("src", LongType), StructField("dst", LongType))))
          .load("/home/enzo/Data/linkedin.edges")
          .toDF("src", "dst")
          .cache(), N_ITER)
        res._1.filter(col("id") === 1072133284 || (col("id") >= 0 && col("id") <= 7)).show(10)
        res._2.foreach(_.unpersist(false))
      }
      println("WAS CUSTOM")

      Utils.time {
        PRGraphXBuiltIn(
          "/home/enzo/Data/linkedin.edges")
      }
      println("WAS GraphX")

    }
  }


  /**
   * Elapsed time: 43256.253605ms
   * (WAS GraphX,6)
   * Elapsed time: 42084.112966ms
   * (WAS GraphX,12)
   * Elapsed time: 43058.250126ms
   * (WAS GraphX,24)
   * Elapsed time: 41263.123503ms
   * (WAS GraphX,36)
   * Elapsed time: 38428.72632ms
   * (WAS GraphX,48)
   * Elapsed time: 38101.886784ms
   * (WAS GraphX,60)
   * Elapsed time: 39493.862827ms
   * (WAS GraphX,180)
   */
  def benchNumPartitionsGraphX(): Unit = {
    N_ITER = 10
    withNumShufflePartition(spark, 12) {
      Utils.time {
        PRGraphXBuiltIn(
          "/home/enzo/Prog/spark/data/graphx/followers.txt")
      }
      println("WAS GraphX PRE", spark.sparkContext.getConf.get("spark.default.parallelism"))
    }
    withNumShufflePartition(spark, 6) {
      Utils.time {
        PRGraphXBuiltIn(
          "/home/enzo/Data/linkedin.edges")
      }
      println("WAS GraphX", spark.sparkContext.getConf.get("spark.default.parallelism"))
    }
    //    withNumShufflePartition(spark, 12) {
    //      Utils.time {
    //        execGraphXNoCache(
    //          "/home/enzo/Data/linkedin.edges",
    //          "\t",
    //          Some("/home/enzo/Data/linkedin.nodes")
    //        )
    //      }
    //      println("WAS GraphX", spark.sparkContext.getConf.get("spark.default.parallelism"))
    //    }
    //    withNumShufflePartition(spark, 24) {
    //      Utils.time {
    //        execGraphXNoCache(
    //          "/home/enzo/Data/linkedin.edges",
    //          "\t",
    //          Some("/home/enzo/Data/linkedin.nodes")
    //        )
    //      }
    //      println("WAS GraphX", spark.sparkContext.getConf.get("spark.default.parallelism"))
    //    }
    //    withNumShufflePartition(spark, 36) {
    //      Utils.time {
    //        execGraphXNoCache(
    //          "/home/enzo/Data/linkedin.edges",
    //          "\t",
    //          Some("/home/enzo/Data/linkedin.nodes")
    //        )
    //      }
    //      println("WAS GraphX", spark.sparkContext.getConf.get("spark.default.parallelism"))
    //    }
    //    withNumShufflePartition(spark, 48) {
    //      Utils.time {
    //        execGraphXNoCache(
    //          "/home/enzo/Data/linkedin.edges",
    //          "\t",
    //          Some("/home/enzo/Data/linkedin.nodes")
    //        )
    //      }
    //      println("WAS GraphX", spark.sparkContext.getConf.get("spark.default.parallelism"))
    //    }
    //    withNumShufflePartition(spark, 60) {
    //      Utils.time {
    //        execGraphXNoCache(
    //          "/home/enzo/Data/linkedin.edges",
    //          "\t",
    //          Some("/home/enzo/Data/linkedin.nodes")
    //        )
    //      }
    //      println("WAS GraphX", spark.sparkContext.getConf.get("spark.default.parallelism"))
    //    }
    //    withNumShufflePartition(spark, 180) {
    //      Utils.time {
    //        execGraphXNoCache(
    //          "/home/enzo/Data/linkedin.edges",
    //          "\t",
    //          Some("/home/enzo/Data/linkedin.nodes")
    //        )
    //      }
    //      println("WAS GraphX", spark.sparkContext.getConf.get("spark.default.parallelism"))
    //    }
  }

  /**
   * Elapsed time: 84424.790608ms
   * (WAS CUSTOM,6)
   *
   * Elapsed time: 79502.713619ms
   * (WAS CUSTOM,12)
   *
   * Elapsed time: 62838.351788ms
   * (WAS CUSTOM,24)
   *
   * Elapsed time: 49968.118358ms
   * (WAS CUSTOM,36)
   *
   * Elapsed time: 49654.073817ms
   * (WAS CUSTOM,48)
   *
   * Elapsed time: 49715.585694ms
   * (WAS CUSTOM,60)
   *
   * Elapsed time: 57246.691278ms
   * (WAS CUSTOM,180)*/
  def benchNumPartitionssqlPR(): Unit = {
    N_ITER = 10
    Utils.time {
      val res = PRpureSQL(spark.read
        .format("csv")
        .option("delimiter", " ")
        .schema(StructType(Seq(StructField("src", LongType), StructField("dst", LongType))))
        .load("/home/enzo/Prog/spark/data/graphx/followers.txt")
        .toDF("src", "dst")
        .cache(), N_ITER)
      res._1.filter(col("id") === 1072133284 || (col("id") >= 0 && col("id") <= 7)).show(10)
      res._2.foreach(_.unpersist(false))
    }
    println("WAS CUSTOM, PRE", spark.sessionState.conf.numShufflePartitions)

    withNumShufflePartition(spark, 6) {
      Utils.time {
        val res = PRpureSQL(spark.read
          .format("csv")
          .option("delimiter", " ")
          .schema(StructType(Seq(StructField("src", LongType), StructField("dst", LongType))))
          .load("/home/enzo/Data/linkedin.edges")
          .toDF("src", "dst")
          .cache(), N_ITER)
        res._1.filter(col("id") === 1072133284 || (col("id") >= 0 && col("id") <= 7)).show(10)
        res._2.foreach(_.unpersist(false))
      }
      println("WAS CUSTOM", spark.sessionState.conf.numShufflePartitions)
    }
    //    withNumShufflePartition(spark, 12) {
    //      Utils.time {
    //        val res = sqlPR(spark.read
    //          .format("csv")
    //          .option("delimiter", " ")
    //          .schema(StructType(Seq(StructField("src", LongType), StructField("dst", LongType))))
    //          .load("/home/enzo/Data/linkedin.edges")
    //          .toDF("src", "dst")
    //          .cache(), N_ITER)
    //        res._1.filter(col("id") === 1072133284 || (col("id") >=0 && col("id") <= 7)).show(10)
    //        res._2.foreach(_.unpersist(false))
    //      }
    //      println("WAS CUSTOM", spark.sessionState.conf.numShufflePartitions)
    //    }
    //    withNumShufflePartition(spark, 24) {
    //      Utils.time {
    //        val res = sqlPR(spark.read
    //          .format("csv")
    //          .option("delimiter", " ")
    //          .schema(StructType(Seq(StructField("src", LongType), StructField("dst", LongType))))
    //          .load("/home/enzo/Data/linkedin.edges")
    //          .toDF("src", "dst")
    //          .cache(), N_ITER)
    //        res._1.filter(col("id") === 1072133284 || (col("id") >=0 && col("id") <= 7)).show(10)
    //        res._2.foreach(_.unpersist(false))
    //      }
    //      println("WAS CUSTOM", spark.sessionState.conf.numShufflePartitions)
    //    }
    //    withNumShufflePartition(spark, 36) {
    //      Utils.time {
    //        val res = sqlPR(spark.read
    //          .format("csv")
    //          .option("delimiter", " ")
    //          .schema(StructType(Seq(StructField("src", LongType), StructField("dst", LongType))))
    //          .load("/home/enzo/Data/linkedin.edges")
    //          .toDF("src", "dst")
    //          .cache(), N_ITER)
    //        res._1.filter(col("id") === 1072133284 || (col("id") >=0 && col("id") <= 7)).show(10)
    //        res._2.foreach(_.unpersist(false))
    //      }
    //      println("WAS CUSTOM", spark.sessionState.conf.numShufflePartitions)
    //    }
    //    withNumShufflePartition(spark, 48) {
    //      Utils.time {
    //        val res = sqlPR(spark.read
    //          .format("csv")
    //          .option("delimiter", " ")
    //          .schema(StructType(Seq(StructField("src", LongType), StructField("dst", LongType))))
    //          .load("/home/enzo/Data/linkedin.edges")
    //          .toDF("src", "dst")
    //          .cache(), N_ITER)
    //        res._1.filter(col("id") === 1072133284 || (col("id") >=0 && col("id") <= 7)).show(10)
    //        res._2.foreach(_.unpersist(false))
    //      }
    //      println("WAS CUSTOM", spark.sessionState.conf.numShufflePartitions)
    //    }
    //    withNumShufflePartition(spark, 60) {
    //      Utils.time {
    //        val res = sqlPR(spark.read
    //          .format("csv")
    //          .option("delimiter", " ")
    //          .schema(StructType(Seq(StructField("src", LongType), StructField("dst", LongType))))
    //          .load("/home/enzo/Data/linkedin.edges")
    //          .toDF("src", "dst")
    //          .cache(), N_ITER)
    //        res._1.filter(col("id") === 1072133284 || (col("id") >=0 && col("id") <= 7)).show(10)
    //        res._2.foreach(_.unpersist(false))
    //      }
    //      println("WAS CUSTOM", spark.sessionState.conf.numShufflePartitions)
    //    }
    //    withNumShufflePartition(spark, 180) {
    //      Utils.time {
    //        val res = sqlPR(spark.read
    //          .format("csv")
    //          .option("delimiter", " ")
    //          .schema(StructType(Seq(StructField("src", LongType), StructField("dst", LongType))))
    //          .load("/home/enzo/Data/linkedin.edges")
    //          .toDF("src", "dst")
    //          .cache(), N_ITER)
    //        res._1.filter(col("id") === 1072133284 || (col("id") >=0 && col("id") <= 7)).show(10)
    //        res._2.foreach(_.unpersist(false))
    //      }
    //      println("WAS CUSTOM", spark.sessionState.conf.numShufflePartitions)
    //    }
  }

  /**
   * N_ITER=1
   * Elapsed time: 18456.667003ms
   * (WAS GraphX,48)
   * Elapsed time: 19235.134488ms
   * (WAS Custom,48)
   * Elapsed time: 21362.89262ms
   * (WAS AggMess,48)
   * Elapsed time: 35702.836235ms
   * (WAS Soundcloud,48)
   * Elapsed time: 85683.263557ms
   * (WAS GraphFrames,48,48)
   *
   * N_ITER=5
   * Elapsed time: 30911.907088ms
   * (WAS GraphX,48)
   * Elapsed time: 42702.515075ms
   * (WAS Custom,48)
   * Elapsed time: 66015.725386ms
   * (WAS AggMess,48)
   * Elapsed time: 97075.823488ms
   * (WAS Soundcloud,48)
   * Elapsed time: 311163.458741ms
   * (WAS GraphFrames,48,48)
   *
   * N_ITER=10
   * Elapsed time: 46336.912898ms
   * (WAS GraphX,48)
   * Elapsed time: 64395.448749ms
   * (WAS Custom,48)
   * Elapsed time: 116064.064586ms
   * (WAS AggMess,48)
   * Elapsed time: 166004.745441ms
   * (WAS Soundcloud,48)
   * Elapsed time: 1400000ms
   * (WAS GraphFrames,48,48)
   */  // TODO: bench pregel GraphFrames
  def benchAll() = {
    val dataPath = "/home/enzo/Data/celineengsrcdst.edges" // "/home/enzo/Data/linkedin.edges"
    N_ITER = 5
    withNumShufflePartition(spark, 48) {
//      Utils.time {
//        PRGraphXBuiltIn(
//          "/home/enzo/Prog/spark/data/graphx/followers.txt")
//      }
//      println("PRE WAS GraphX", spark.sparkContext.getConf.get("spark.default.parallelism"))

//      Utils.time {
//        val res = PRpureSQL(spark.read
//          .format("csv")
//          .option("delimiter", " ")
//          .schema(StructType(Seq(StructField("src", LongType), StructField("dst", LongType))))
//          .load("/home/enzo/Prog/spark/data/graphx/followers.txt")
//          .toDF("src", "dst")
//          .cache(), N_ITER)
//        res._1.filter(col("id") === 1072133284 || (col("id") >= 0 && col("id") <= 7)).show(10)
//        res._2.foreach(_.unpersist(false))
//      }
//      println("PRE WAS Custom", spark.sessionState.conf.numShufflePartitions)

      Utils.time {
        PRGraphXBuiltIn(
          dataPath)
      }
      println("WAS GraphX", spark.sparkContext.getConf.get("spark.default.parallelism"))
//      Utils.time {
//        val res = PRpureSQLpart(spark.read
//          .format("csv")
//          .option("delimiter", " ")
//          .schema(StructType(Seq(StructField("src", LongType), StructField("dst", LongType))))
//          .load(dataPath)
//          .toDF("src", "dst")
//          .cache(), N_ITER)
//        res._1.filter(col("id") === 1072133284 || (col("id") >= 0 && col("id") <= 7)).show(10)
//        res._2.foreach(_.unpersist(false))
//      }
//      println("WAS Custom partition effort", spark.sessionState.conf.numShufflePartitions)
//
//      Utils.time {
//        val res = PRpureSQL(spark.read
//          .format("csv")
//          .option("delimiter", " ")
//          .schema(StructType(Seq(StructField("src", LongType), StructField("dst", LongType))))
//          .load(dataPath)
//          .toDF("src", "dst")
//          .cache(), N_ITER)
//        res._1.filter(col("id") === 1072133284 || (col("id") >= 0 && col("id") <= 7)).show(10)
//        res._2.foreach(_.unpersist(false))
//      }
//      println("WAS Custom", spark.sessionState.conf.numShufflePartitions)
//
//      Utils.time {
//        AggMess(spark.read
//          .format("csv")
//          .option("delimiter", " ")
//          .schema(StructType(Seq(StructField("src", LongType), StructField("dst", LongType))))
//          .load(dataPath)
//          .toDF("src", "dst")
//          .cache())
//      }
//      println("WAS AggMess", spark.sessionState.conf.numShufflePartitions)
//
//      Utils.time {
//        PRSoundCloud(
//          dataPath,
//          "\t",
//          null
//        )
//      }
//      println("WAS Soundcloud", spark.sparkContext.getConf.get("spark.default.parallelism"))

      //      Utils.time {
      //        PRGraphFramesBuiltIn(
      //          "/home/enzo/Data/linkedin.edges",
      //          "\t",
      //          Some("/home/enzo/Data/linkedin.nodes"),
      //          Seq("id", "pseudo")
      //        )
      //      }
      //      println("WAS GraphFrames", spark.sparkContext.getConf.get("spark.default.parallelism"), spark.sessionState.conf.numShufflePartitions)
    }
  }

  override def run(): Unit = {
    benchAll()
    while (true) {
      Thread.sleep(1000)
    }
  }

}
