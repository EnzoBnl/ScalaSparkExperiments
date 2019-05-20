package com.enzobnl.sparkscalaexpe.playground

import com.enzobnl.sparkscalaexpe.util.QuickSQLContextFactory
import org.apache.spark.ml.feature.SQLTransformer

import scala.util.Random
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.functions._

object Sandbox extends Runnable {
  lazy val spark: SQLContext = QuickSQLContextFactory.getOrCreate()
  import spark.implicits._
  override def run(): Unit = {
    val df = spark.createDataFrame(
      Seq(("Thin", "Cell", 6000),
        ("Normal", "Tablet", 1500),
        ("Mini", "Tablet", 5500),
        ("Ultra thin", "Cell", 5000),
        ("Very thin", "Cell", 6000),
        ("Big", "Tablet", 2500),
        ("Bendable", "Cell", 3000),
        ("Foldable", "Cell", 3000),
        ("Pro", "Tablet", 4500),
        ("Pro2", "Tablet", 6500))).toDF("product", "category", "revenue")
    df.createOrReplaceGlobalTempView("temp")
//    spark.sql("SELECT * FROM(SELECT *, row_number() OVER (PARTITION BY category ORDER BY revenue DESC) AS rank FROM global_temp.temp) WHERE rank <= 2").show()
//    spark.sql(
//      "SELECT *,  best - revenue AS diff FROM "+
//        "(SELECT *, max(revenue) OVER (PARTITION BY category AS best FROM global_temp.temp)"
//            ).show()
//    spark.sql(
//      "SELECT *,  best - revenue AS diff FROM "+
//        "(SELECT *, max(revenue) OVER (PARTITION BY category ORDER BY revenue 1 PRECEDING) AS best FROM global_temp.temp)"
//    ).show()
    spark.udf.register("f1", (row: Row) => row)
    spark.sql("SELECT * from global_temp.temp").show()
  }
}
