package com.enzobnl.playground
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.{SQLContext, SparkSession}
import vegas._
import vegas.render.WindowRenderer._
import vegas.sparkExt._
object Exercice {
  def main(args: Array[String]): Unit={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[*]")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    val df = spark.read.option("header", true).option("delimiter", "\t").csv("C:/Applications/khiops/samples/Adult/Adult.txt")
    print(df.schema)
    val plot = Vegas("Country Pop").
      withDataFrame(df.limit(5)).
      encodeX("Label", Nom).
      encodeY("age", Quant).
      mark(Bar)
    plot.show
  }
}
