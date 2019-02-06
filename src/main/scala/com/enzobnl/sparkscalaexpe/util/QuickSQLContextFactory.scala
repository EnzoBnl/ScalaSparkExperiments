package com.enzobnl.sparkscalaexpe.util
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object QuickSQLContextFactory{
  private var appName: String = "default"
  private lazy val sqlContext: SQLContext = new SQLContext(new SparkContext(new SparkConf().setAppName(appName).setMaster("local[*]").set("spark.driver.memory", "3g")))
  def getOrCreate(appName: String): SQLContext = {
    this.appName = appName
    this.sqlContext
  }
}
