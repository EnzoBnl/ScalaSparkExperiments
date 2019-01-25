package com.enzobnl.sparkscalaexpe.wordcountsparkscala

import org.apache.spark._
import org.apache.spark.SparkContext._
//import org.apache.spark.sql.SQLContext.implicits._
object WordCount {
  def main(args: Array[String]) {
    val inputFile = args(0)
    val outputFile = args(1)
    val conf = new SparkConf().setAppName("wordCount")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    // Load our input data.
    val input =  sc.textFile(inputFile)
    // Split up into words.
    val words = input.flatMap(line => line.split(" "))
    // Transform into word and count.
    val counts = words.map(word => (word, 1)).reduceByKey(_ + _)
    // Save the word count back out to a text file, causing evaluation.
    counts.sortBy{case (_, value) => value}.repartition(1).saveAsTextFile(outputFile)
  }
}