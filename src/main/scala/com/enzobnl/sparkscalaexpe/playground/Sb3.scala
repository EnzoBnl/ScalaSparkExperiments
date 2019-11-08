package com.enzobnl.sparkscalaexpe.playground

import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object Sb3 extends Runnable {
  final val DAMPING_FACTOR: Double = 0.85
  var N_ITER: Int = 3
  lazy val spark: SparkSession = {
    val spark =
      SparkSession
        .builder
        .config("spark.default.parallelism", "12")
        .master("local[*]")
        .appName("GraphxVsGraphFramesPageRanksApp")
        .getOrCreate
    spark.sparkContext.setLogLevel("ERROR")
    spark
  }



  override def run(): Unit = {
    val df6mois_vente = spark.read
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .format("csv")
      .load("./immo/6mois_vente.csv")
      .toDF("ville", "evolution", "price_m2")

    val df6mois_location = spark.read
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .format("csv")
      .load("./immo/6mois_location.csv")
      .toDF("ville", "evolution", "price_m2")

    val df3mois_vente = spark.read
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .format("csv")
      .load("./immo/3mois_vente.csv")
      .toDF("ville", "evolution", "price_m2")

    val df3mois_location = spark.read
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .format("csv")
      .load("./immo/3mois_location.csv")
      .toDF("ville", "evolution", "price_m2")

    df6mois_location
      .join(df6mois_vente, Seq("ville"), "inner")
      .withColumn("renta", df6mois_location("price_m2")/df6mois_vente("price_m2"))
      .orderBy(col("renta").desc)
      .repartition(1)
      .withColumn("rank", monotonically_increasing_id())
      .select("rank", "ville", "renta")
      .show(Int.MaxValue)

    df3mois_location
      .join(df3mois_vente, Seq("ville"), "inner")
      .withColumn("renta", df3mois_location("price_m2")/df3mois_vente("price_m2"))
      .orderBy(col("renta").desc)
      .repartition(1)
      .withColumn("rank", monotonically_increasing_id())
      .show(Int.MaxValue)
  }
}

