package com.enzobnl.sparkscalaexpe.playground

import com.enzobnl.sparkscalaexpe.util.Utils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._

/**
 * import sys
 * from pyspark.sql.window import Window
 * import pyspark.sql.functions as func
 * windowSpec = \
 * Window
 * .partitionBy(df['category']) \
 * .orderBy(df['revenue'].desc()) \
 * .rangeBetween(-sys.maxsize, sys.maxsize)
 * dataFrame = sqlContext.table("productRevenue")
 * revenue_difference = \
 * (func.max(dataFrame['revenue']).over(windowSpec) - dataFrame['revenue'])
 * dataFrame.select(
 * dataFrame['product'],
 * dataFrame['category'],
 * dataFrame['revenue'],
 * revenue_difference.alias("revenue_difference"))
 */
object Sb3 extends Runnable {
  final val DAMPING_FACTOR: Double = 0.85
  var N_ITER: Int = 3
  lazy val spark: SparkSession = SparkSession
    .builder
    .config("spark.default.parallelism", "12")
    .master("local[*]")
    .appName("GraphxVsGraphFramesPageRanksApp")
    .getOrCreate

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  override def run(): Unit = {

    Utils.time {
      PR(spark
        .createDataFrame(
          Seq(
            (1, 2),
            (1, 3),
            (2, 3),
            (3, 4))
        )
        .toDF("src", "dst"))
    }

  }

  def PR(edges: DataFrame, nIter: Int = N_ITER, verbose: Boolean = false): Unit = {
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
          .withColumn("PR", lit(1 - DAMPING_FACTOR) + lit(DAMPING_FACTOR) * expr("ifnull(sumSent, 0)"))
          .drop("sumSent").cache()
      if (verbose) vertices.show()
      //              .groupBy("dst")
      //              .sum("toSend")
      //              .withColumnRenamed("dst", "id").as("b")
      //              ,
      //            $"a.src" === $"b.id",
      //            "outer"
      //          )
      //            .drop("id")
      //            .withColumnRenamed("sum(toSend)", "sumSent")
      //            .withColumn("PR", lit(1-DANGLING_FACTOR)*lit(DANGLING_FACTOR)*expr("ifnull(sumSent, 0)"))
    }
    //    vertices.show()
    //    vertices = vertices.join(

    vertices.show(10)
  }
}

