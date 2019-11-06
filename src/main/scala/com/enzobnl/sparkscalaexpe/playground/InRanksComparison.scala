package com.enzobnl.sparkscalaexpe.playground

import cc.p2k.spark.graphx.lib.HarmonicCentrality
import com.enzobnl.sparkscalaexpe.playground.Sb3.spark
import com.enzobnl.sparkscalaexpe.util.Utils
import com.soundcloud.spark.pagerank.PageRankGraph
import ml.sparkling.graph.api.operators.measures.VertexMeasureConfiguration
import ml.sparkling.graph.operators.measures.vertex.closenes.Closeness
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.graphx.{Edge, EdgeDirection, EdgeTriplet, Graph, GraphLoader, PartitionStrategy, Pregel, TripletFields, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, SparkUtils}
import org.apache.spark.sql.SparkUtils.withNumShufflePartition
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StructField, StructType}

trait InRank extends Serializable {
  def apply(edgesPath: String): Dataset[_]
}

trait InRankSpark extends InRank {
  val spark: SparkSession
  spark.sparkContext.setCheckpointDir("./checkpoint")
}

trait InRankGraphX extends InRankSpark {
  private var partitioner_ : Option[PartitionStrategy] = Some(PartitionStrategy.EdgePartition2D)

  def partitioner: Option[PartitionStrategy] = partitioner_

  def partitioner_=(partition: Option[PartitionStrategy]): Unit = partitioner_ = partition

  /**
   * put a Graph[Int, Int] in cache from edgesPath according to this._partitioner
   *
   * @param edgesPath
   * @return
   */
  def getGraph(edgesPath: String): Graph[Int, Int] = {
    val graphNotPartitioned = GraphLoader.edgeListFile(spark.sparkContext, edgesPath)
    val graph = (partitioner_ match {
      case Some(p) => graphNotPartitioned.partitionBy(p)
      case None => {
        println("aaa");
        graphNotPartitioned
      }
    }).cache()
    graph.edges.foreachPartition(_ => {})
    graph
  }
}

trait PRPureSQLIter extends InRankSpark {
  /*
  Finally, we can optimize communication in PageRank
by controlling the partitioning of the RDDs. If we spec-
ify a partitioning for links (e.g., hash-partition the link
lists by URL across nodes), we can partition ranks in
the same way and ensure that the join operation between
links and ranks requires no communication (as each
URL’s rank will be on the same machine as its link list).
We can also write a custom Partitioner class to group TODO
pages that link to each other together (e.g., partition the TODO
URLs by domain name).TODO
   */

  val nIter: Int

  def algo(e: DataFrame, nIter: Int, d: Double = 0.85): (DataFrame, Seq[DataFrame])

  override def apply(edgesPath: String): Dataset[_] = {
    val df = spark.read
      .format("csv")
      .option("delimiter", " ")
      .schema(StructType(Seq(StructField("src", LongType), StructField("dst", LongType))))
      .load(edgesPath)
      .toDF("src", "dst").cache()

    df.count()

    Utils.time {
      val res = algo(df, nIter)
      val sum: Double = res._1.groupBy().sum("PR").collect()(0).getAs(0)
      val count = res._1.count()
      val finalRes = res._1.select(col("id"), col("PR") / lit(sum) * lit(count))
      finalRes.cache().count()
      res._2.foreach(_.unpersist(false))
      finalRes
    }.toDF("id", "score").orderBy(col("score").desc)
  }
}

class PRPureSQLIterSimple(override val spark: SparkSession, override val nIter: Int) extends PRPureSQLIter {
  def algo(e: DataFrame, nIter: Int, d: Double = 0.85): (DataFrame, Seq[DataFrame]) = {
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
      .withColumn("outDegree", expr("ifnull(count, 1)"))
      .cache()

    // id, PRtoSend
    var vertices = idsAndOutDegrees.withColumn("PRtoSend", lit(1) / col("outDegree"))

    for (i <- 1 to nIter) {

      // src, dst, PRtoSend
      val weightedEdges = edges.join(vertices, edges("src") === vertices("id"), "inner").drop("id")
      weightedEdges.show()
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
}

class PRPureSQLIterPartitioned(override val spark: SparkSession, override val nIter: Int) extends PRPureSQLIter {
  def algo(e: DataFrame, nIter: Int, d: Double = 0.85): (DataFrame, Seq[DataFrame]) = {
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
      .withColumn("outDegree", expr("ifnull(count, 1)"))
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
}

class PRPureSQLIterNoVertices(override val spark: SparkSession, override val nIter: Int) extends PRPureSQLIter {
  def algo(e: DataFrame, nIter: Int, d: Double = 0.85): (DataFrame, Seq[DataFrame]) = {
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
}

class PRPureSQLIterNoVerticesPartitioned(override val spark: SparkSession, override val nIter: Int) extends PRPureSQLIter {
  def algo(e: DataFrame, nIter: Int, d: Double = 0.85): (DataFrame, Seq[DataFrame]) = {
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
      .repartition(col("src"))
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
}

class PRGraphXIter(override val spark: SparkSession, nIter: Int) extends InRankGraphX {
  def runWithOptions(graph: Graph[Int, Int], numIter: Int, resetProb: Double): Graph[Double, Double] = {
    // Initialize the PageRank graph with each edge attribute having
    // weight 1/outDegree and each vertex with attribute 1.0. // When running personalized pagerank, only the source vertex // has an attribute 1.0. All others are set to 0.
    var rankGraph: Graph[Double, Double] = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }
      // Set the weight on the edges based on the degree
      .mapTriplets(e => 1.0 / e.srcAttr, TripletFields.Src)
      // Set the vertex attributes to the initial pagerank values
      .mapVertices { (id, attr) => 1.0 }
    var iteration = 0
    var prevRankGraph: Graph[Double, Double] = null
    while (iteration < numIter) {
      rankGraph.cache()
      // Compute the outgoing rank contributions of each vertex, perform local preaggregation, and
      // do the final aggregation at the receiving vertices. Requires a shuffle for aggregation.
      val rankUpdates = rankGraph.aggregateMessages[Double](
        ctx => ctx.sendToDst(ctx.srcAttr * ctx.attr), _ + _, TripletFields.Src)

      // Apply the final rank updates to get the new ranks, using join to preserve ranks of vertices
      // that didn't receive a message. Requires a shuffle for broadcasting updated ranks to the
      // edge partitions.
      prevRankGraph = rankGraph
      rankGraph = rankGraph.outerJoinVertices(rankUpdates) {
        (id, oldRank, msgSumOpt) => resetProb + (1.0 - resetProb) * msgSumOpt.getOrElse(0.0)
      }.cache()

      rankGraph.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices
      prevRankGraph.vertices.unpersist(false)
      prevRankGraph.edges.unpersist(false)
      iteration += 1
    }
    // SPARK-18847 If the graph has sinks (vertices with no outgoing edges) correct the sum of ranks
    val rankSum = rankGraph.vertices.values.sum()
    val numVertices = rankGraph.numVertices
    val correctionFactor = numVertices.toDouble / rankSum
    rankGraph.mapVertices((id, rank) => rank * correctionFactor)
  }

  override def apply(edgesPath: String): Dataset[_] = {
    import spark.implicits._
    val graph = getGraph(edgesPath)
    Utils.time {
      val prResult = runWithOptions(graph, nIter, 0.15)
      prResult.cache().edges.foreachPartition(_ => {})
      //      println(prResult.vertices.toDebugString)
      prResult.vertices
    }.toDF("id", "score").orderBy(col("score").desc)
  }
}

class PRGraphXConv(override val spark: SparkSession, tol: Double) extends InRankGraphX {
  override def apply(edgesPath: String): Dataset[_] = {
    import spark.implicits._
    val graph = getGraph(edgesPath)
    Utils.time {
      val prResult = graph.pageRank(tol, 0.15)
      prResult.cache().edges.foreachPartition(_ => {})
      //      println(prResult.vertices.toDebugString)
      prResult.vertices
    }.toDF("id", "score").orderBy(col("score").desc)
  }
}

class PRSoundCloud(override val spark: SparkSession, nIter: Int) extends InRankGraphX {

  override def apply(edgesPath: String): Dataset[_] = {
    import spark.implicits._
    val edgesRDD = spark.read
      .format("csv")
      .option("delimiter", " ")
      .schema(StructType(Seq(StructField("src", LongType), StructField("dst", LongType))))
      .load(edgesPath)
      .toDF("src", "dst")
      .map(row => com.soundcloud.spark.pagerank.Edge(row.getAs(0), row.getAs(0), 1))
      .rdd
    val g: PageRankGraph = com.soundcloud.spark.pagerank.PageRankGraph.fromEdgesWithUniformPriors(edgesRDD, StorageLevels.MEMORY_ONLY, StorageLevels.MEMORY_AND_DISK, StorageLevels.MEMORY_AND_DISK)
    g.edges.cache().foreachPartition(_ => ())
    g.vertices.cache().foreachPartition(_ => ())

    Utils.time {
      val df = com.soundcloud.spark.pagerank.PageRank.run(g, 0.15, nIter)
        .toDF("id", "score")
      val sum: Double = df.groupBy().sum("score").collect()(0).getAs(0)
      val count = df.count()
      val finalRes = df.select(col("id"), (col("score") / lit(sum) * lit(count)).as("score"))
      finalRes.cache().count()
      finalRes
    }.orderBy(col("score").desc)
  }
}

class HCp2k(override val spark: SparkSession) extends InRankGraphX {

  override def apply(edgesPath: String): Dataset[_] = {
    import spark.implicits._
    val graph = getGraph(edgesPath)
    Utils.time {
      val ranks = HarmonicCentrality.harmonicCentrality(graph).vertices.cache()
      ranks.count()
      val df = ranks.toDF("id", "score")
      val sum: Double = df.groupBy().sum("score").collect()(0).getAs(0)
      val count = df.count()
      df.select(col("id"), col("score") / lit(sum) * lit(count)).toDF("id", "score")
    }.orderBy(col("score").desc)
  }
}

class HCSparklingGraphs(override val spark: SparkSession) extends InRankGraphX {
  // /!\ NOT ACCURATE ?
  override def apply(edgesPath: String): Dataset[_] = {
    import spark.implicits._
    val graph = getGraph(edgesPath)

    Utils.time {
      val ranks = Closeness.computeHarmonic(graph, VertexMeasureConfiguration()).vertices.cache()
      ranks.count()
      val df = ranks.toDF("id", "score")
      val sum: Double = df.groupBy().sum("score").collect()(0).getAs(0)
      val count = df.count()
      df.select(col("id"), col("score") / lit(sum) * lit(count)).toDF("id", "score")
    }.orderBy(col("score").desc)
  }

}

case class VertexState(
                        val depth: Int,
                        val redir: Set[VertexId] = Set(),
                        val score: Double = 0.0f,
                        val outDegree: Int = 0
                      ) {

  def addRedir(v: VertexId) = this.copy(redir = this.redir + v)

  def addRedirs(vs: Set[VertexId]) = this.copy(redir = this.redir ++ vs)

  def addScore(score: Double) = this.copy(score = this.score + score)

  def setScore(score: Double) = this.copy(score = score)

  def setOutDeg(deg: Int) = this.copy(outDegree = deg)

  def getScoreToDistribute = this.score / this.outDegree
}

class HCOpic(override val spark: SparkSession, home: VertexId, maxDepth: Int) extends InRankGraphX {
  def computeDepth(graph: Graph[Int, Int], maxIter: Int): Dataset[(VertexId, Int)] = {
    import spark.implicits._
    val graphInit = graph
      .mapVertices((id, _) => if (id == home) {
        println("HOME FOUND")
        0
      } else {
        Int.MaxValue
      })

    graphInit.cache().edges.foreachPartition(_ => ())
    graphInit
      .pregel(Int.MaxValue, maxIterations = maxIter)(
        (vertexId: VertexId, attr: Int, messageAggregated: Int) => {
          if (vertexId == home) attr
          else Math.min(attr, messageAggregated)
        },
        (edge: EdgeTriplet[Int, Int]) => {
          if (edge.srcAttr != Int.MaxValue) Iterator((edge.dstId, edge.srcAttr + 1))
          else Iterator.empty
        },
        (a, b) => {
          Math.min(a, b)
        }
      )
      .vertices.toDS
  }

  override def apply(edgesPath: String): Dataset[_] = {
    import spark.implicits._
    val graph: Graph[Int, Int] = getGraph(edgesPath)
    val depths: Dataset[(VertexId, Int)] = computeDepth(graph, maxDepth)
//    depths.toDF("id", "d").where(col("d") !== lit(2147483647)).groupBy().max("d").show()
    println("N° orphans", depths.toDF("id", "d").where(col("d") === lit(2147483647)).count())
    println("max depth set", maxDepth)
    println("Real max depth except orphans", depths.toDF("id", "d").where(col("d") !== lit(2147483647)).groupBy().max("d").collect()(0).getAs(0))
    (1 to 9).foreach(i => println(s"pages depth max $i", depths.toDF("id", "d").where(col("d") <= lit(i)).count()))

    val graphVertexState: Graph[VertexState, Int] =
      Graph[VertexState, Int](
        depths.map(idAndDepth => (idAndDepth._1, VertexState(idAndDepth._2))).rdd,
        graph.edges
      ).cache()
    graphVertexState.edges.foreachPartition(_ => ())

    val opicGraph = new OpicGraph(graphVertexState)

    Utils.time {
      val ranks: RDD[(VertexId, VertexState)] = opicGraph.computeOpic(maxDepth).vertices.cache()
      val df = ranks.map(e => (e._1, e._2.score)).toDF("id", "score")
      val sum: Double = df.groupBy().sum("score").collect()(0).getAs(0)
      val count = df.count()
      df.select(col("id"), col("score") / lit(sum) * lit(count)).toDF("id", "score")
    }.orderBy(col("score").desc)
  }

  /**
   * Specialized Graph to compute the Opic score.
   * Vertex attributes are of type {@link VertexState},
   * which contains all vertex information useful to the algorithm.
   * Edge attributes are {@link Int} counting the weight of each Edge.
   *
   * @param graph
   */
  class OpicGraph(graph: Graph[VertexState, Int]) extends Serializable {

    def computeOpic(maxDepth: Int): Graph[VertexState, Int] = {
      graph.persist()
      graph.edges.foreachPartition(x => {})

      // Initialization : compute vertex outDegree and init score
      var gWithScore: Graph[VertexState, Int] = initGraphState

      gWithScore.persist()
      gWithScore.edges.foreachPartition(_ => {})
      graph.unpersist(false)
      var prevG: Graph[VertexState, Int] = null

      for (d <- 0 to maxDepth) {
        // Extract subgraph up to the current depth
        // Only edges from this subgraph will propagate score
        val depthSubgraph = gWithScore.subgraph(
          edge => edge.srcAttr.depth == d,
          (_, state) => state.depth <= d + 1
        )

        // Create score propagation messages
        val messages = depthSubgraph.triplets
          .flatMap(OpicGraph.sendScoreToAllRedirs)

        // Sum score going to each vertex
        val aggregatedMessages = gWithScore.vertices
          .aggregateUsingIndex[Double](messages, _ + _)

        // Update score
        prevG = gWithScore
        gWithScore = prevG.outerJoinVertices(aggregatedMessages) {
          case (id, state, Some(newScore)) => state.addScore(newScore)
          case (id, state, None) => state
        }.persist()

        if (d >= 100 && d % 100 == 0) {
          gWithScore.checkpoint()
        }

        gWithScore.edges.foreachPartition(_ => {})
        prevG.vertices.unpersist(false)
        prevG.edges.unpersist(false)
      }

      gWithScore
    }

    private def initGraphState: Graph[VertexState, Int] = {
      val vertexOutArity = graph.aggregateMessages[Int](
        e => e.sendToSrc(e.attr),
        _ + _,
        TripletFields.EdgeOnly
      )

      var gWithScore = graph.joinVertices(vertexOutArity) {
        case (id, state, outDeg) =>
          state.setScore(if (state.depth == 0) 1 else 0).setOutDeg(outDeg)
      }
      gWithScore
    }
  }

  object OpicGraph extends Serializable {
    implicit def graph2Opic(value: Graph[VertexState, Int]): OpicGraph = {
      new OpicGraph(value)
    }

    def sendScoreToAllRedirs(edge: EdgeTriplet[VertexState, Int]) = {
      val scoreToSend = edge.srcAttr.getScoreToDistribute * edge.attr
      val dstVertices = edge.dstAttr.redir + edge.dstId
      dstVertices.map(id => (id, scoreToSend))
    }

  }


}

object InRanksComparison extends Runnable {
  lazy val spark: SparkSession = SparkSession
    .builder
    .config("spark.default.parallelism", (Runtime.getRuntime().availableProcessors() * 4).toString)
    .config("spark.sql.shuffle.partitions", Runtime.getRuntime().availableProcessors() * 4)
    .master("local[*]")
    .appName("OnCrawlInRankBenches")
    .getOrCreate
  spark.sparkContext.setLogLevel("ERROR")

  def sb() = {
    /*
Elapsed time: 19931.326074ms
+-----------+------------------+
|         id|             score|
+-----------+------------------+
|-1662959015|10464.132450382443|
|-2003865051| 85.37889625418954|
|  507021837| 75.26437757186743|
| -283901582| 75.26437757186743|
| -855495302| 75.26437757186743|
|-1682326457| 45.70762895376608|
| -975678757| 35.94412740629397|
|   17734849| 35.94412740629397|
| 1297996732| 35.94412740629397|
| 2140845240| 35.94412740629397|
|-1860547016| 35.94412740629397|
| 1754636154| 35.94412740629397|
|-1368846206| 35.94412740629397|
| -626035114| 35.94412740629397|
|  644118136| 35.94412740629397|
|-1531059043| 35.94412740629397|
| 2012271522| 35.94412740629397|
| -260906578| 35.94412740629397|
|-1204488259| 35.94412740629397|
|-1340476195| 35.94412740629397|
+-----------+------------------+

     */

    //        val dataPath = "/home/enzo/Data/little3morecentralthanhome.edges" //
//    val dataPath = "/home/enzo/Data/little8newhome.edges"
//val dataPath = "/home/enzo/Data/little3morecentralthanhomebutspreaditscentrality.edges"
    //    val dataPath = "/home/enzo/Data/little.edges" // home 0
        val dataPath = "/home/enzo/Data/celineengsrcdst.edges" // home -1662959015 96,7 MB, 4397207 edges, 32276 vertices
    //    val dataPath = "/home/enzo/Data/linkedin.edges"

    withNumShufflePartition(spark, 48) {

      //      spark.read.csv("/home/enzo/Data/celineeng.edges").filter(col("_c0").contains("-1662959015")).show(false)
      // 100: Elapsed time: 19359.737028ms 24608.352239 19931.326074ms 21314
      // 50: 7946 7457 7537 7377
      // 20: 4367 4242 4084
      // 10 : 5734 3663 3619 3535 3271 3644
      new HCOpic(spark, -1662959015, 10)(dataPath).show()//.selectExpr("id", "round(score, 2)").show()

      //      println("HCp2k")
//      new HCp2k(spark)(dataPath).show()
      //      println("PRGraphXConv")
      // 0.000000001: 195.50258030555153: 27544
      // 0.00000001: 195.5025714393273: 25984
      // 0.0000001: 195.50248674416864: 22608
      // 0.000001: 195.50168786186242: Elapsed time: 16114.451741ms 19323.187472 17090.451325ms
      // 0.00001: 195.49415018840776: 18225 11451 13064
      // 0.0001: 195.42529076711384: 9130 8668
      // 0.001: 195.0453129605576: 6081 6615 5992
      // 0.01: 191.39: 3852
      new PRGraphXConv(spark, 0.000001)(dataPath).show()
      Seq(1, 5, 6, 8, 10, 14, 18, 25).foreach(i => {println(i); new PRGraphXIter(spark, i)(dataPath).show()})
//            new PRGraphXIter(spark, 100)(dataPath).show()

    }
  }

  override def run(): Unit = {
    sb()

    while (true) {
      Thread.sleep(1000)
    }
  }

  def compareShuffleAmountOnPRs() = {
    val dataPath = "/home/enzo/Data/celineengsrcdst.edges" // 96,7 MB, 4397207 edges, 32276 vertices
    //    val dataPath = "/home/enzo/Data/linkedin.edges"
    withNumShufflePartition(spark, 48) {

      // driver	192.168.1.61:42577	Active	120	53.9 MB / 2.9 GB	0.0 B	12	0	0	1443	1443	1.8 min (3 s)	567.4 MB	357.9 MB	351.6 MB
      //      new PRPureSQLIterSimple(spark, 5)(dataPath).show()

      // driver	192.168.1.61:46005	Active	72	52.9 MB / 2.9 GB	0.0 B	12	0	0	1035	1035	2.7 min (5 s)	568.7 MB	272.3 MB	272.3 MB
      //      new PRPureSQLIterNoVertices(spark, 5)(dataPath).show()

      // driver	192.168.1.61:33711	Active	120	53.4 MB / 2.9 GB	0.0 B	12	0	0	963	963	1.7 min (4 s)	379.7 MB	97.7 MB	91.7 MB
      new PRPureSQLIterPartitioned(spark, 5)(dataPath).show()

      // driver	192.168.1.61:37117	Active	27	330.2 MB / 2.9 GB	0.0 B	12	0	0	84	84	25 s (2 s)	1.2 GB	54.4 MB	54.1 MB
      //      new PRGraphXIter(spark, 5)(dataPath).show()

      // driver	192.168.1.61:44541	Active	21	201.4 MB / 2.9 GB	0.0 B	12	0	0	78	78	12 s (0.6 s)	1.1 GB	17.1 MB	16.4 MB
      //      val pr = new PRGraphXIter(spark, 5)
      //      pr.partitioner = None
      //      pr(dataPath).show()

      // driver	192.168.1.61:46245	Active	72	52.7 MB / 2.9 GB	0.0 B	12	0	0	1035	1035	2.7 min (4 s)	568.7 MB	272.7 MB	272.7 MB
      //      new PRPureSQLIterNoVerticesPartitioned(spark, 5)(dataPath).show()

    }
  }

  /**
   * first line of bench is on PRGraphXConv(spark, 0.005)
   * second: PRGraphXConv(spark, 0.000001)
   */
  def compareGraphXPartitionStrategies() = {
    val dataPath = "/home/enzo/Data/celineengsrcdst.edges" // 96,7 MB, 4397207 edges, 32276 vertices (212 orphans)
    //    val dataPath = "/home/enzo/Data/linkedin.edges"
    withNumShufflePartition(spark, 48) {

      // Show entries
      //Search:
      //Executor ID///Address///Status///RDD Blocks///Storage Memory///Disk Used///Cores///Active Tasks///Failed Tasks///Complete Tasks///Total Tasks///Task Time (GC Time) ///Input///Shuffle Read///Shuffle Write///Thread Dump


      val pr = new PRGraphXConv(spark, 0.000001)

      // driver///192.168.1.61:37519///Active///30///287.3 MB / 2.9 GB///0.0 B///12///0///0///420///420///22 s (0.9 s)///3.3 GB///54.8 MB///54.2 MB
      //      driver	192.168.43.31:39457	Active	24	379.1 MB / 428.8 MB	21 KB	12	0	0	1050	1050	1.0 min (5 s)	8.1 GB	213.7 MB	213.1 MB
      // partitionBy (foreach job of two stages) MB ReadWrite: 0
      //      pr.partitioner = None
      //      pr(dataPath).show()

      // driver///192.168.1.61:45965///Active///33///338.1 MB / 4.1 GB///0.0 B///12///0///0///426///426///36 s (1 s)///3.2 GB///88.4 MB///87.9 MB
      //      driver	192.168.43.31:35513	Active	31	298.1 MB / 428.8 MB	31 KB	12	0	0	339	339	53 s (6 s)	2.6 GB	114.9 MB	116.3 MB
      // partitionBy (foreach job of two stages) MB ReadWrite: 41.2
      //            pr.partitioner = Some(PartitionStrategy.EdgePartition2D)
      //            pr(dataPath).show()

      // driver///192.168.1.61:38631///Active///33///336.3 MB / 4.1 GB///0.0 B///12///0///0///426///426///35 s (1 s)///3.2 GB///91.9 MB///91.4 MB
      // driver	192.168.43.31:42719	Active	23	330.4 MB / 428.8 MB	31 KB	12	0	0	1056	1056	1.3 min (8 s)	7.8 GB	223.6 MB	223.1 MB
      // partitionBy (foreach job of two stages) MB ReadWrite: 42.1
      //            pr.partitioner = Some(PartitionStrategy.EdgePartition1D)
      //            pr(dataPath).show()

      // driver///192.168.1.61:44729///Active///33///346.4 MB / 4.1 GB///0.0 B///12///0///0///426///426///37 s (0.9 s)///3.3 GB///102.2 MB///101.6 MB
      // driver	192.168.43.31:35607	Active	22	321.7 MB / 428.8 MB	28.6 KB	12	0	0	1056	1056	1.2 min (7 s)	8.2 GB	261.1 MB	260.5 MB
      // partitionBy (foreach job of two stages) MB ReadWrite: 44.6
      //            pr.partitioner = Some(PartitionStrategy.RandomVertexCut)
      //            pr(dataPath).show()

      // driver///192.168.1.61:41485///Active///33///346.4 MB / 4.1 GB///0.0 B///12///0///0///426///426///33 s (1 s)///3.3 GB///102.2 MB///101.6 MB
      // driver	192.168.43.31:37559	Active	25	398.7 MB / 428.8 MB	31 KB	12	0	0	1056	1056	1.1 min (6 s)	8.2 GB	261.2 MB	260.5 MB
      // partitionBy (foreach job of two stages) MB ReadWrite: 44.6
      pr.partitioner = Some(PartitionStrategy.CanonicalRandomVertexCut)
      pr(dataPath).show()


    }

  }
}
