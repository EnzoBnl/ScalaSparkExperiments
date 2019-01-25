package com.enzobnl.sparkscalaexpe.util
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object QuickSQLContextFactory{
  var appName: String = "default"
  lazy val sqlContext: SQLContext = new SQLContext(new SparkContext(new SparkConf().setAppName(appName).setMaster("local[*]")))
}
