package com.enzobnl.sparkscalaexpe.playground

import com.enzobnl.sparkscalaexpe.util.QuickSQLContextFactory

object Exercice {
  def main(args: Array[String]): Unit={
    QuickSQLContextFactory.appName = "codeGenExpe"
    val spark = QuickSQLContextFactory.sqlContext
    val df = spark.read.option("header", true).option("delimiter", "\t").csv("C:/Applications/khiops/samples/Adult/Adult.txt")
    df.createOrReplaceGlobalTempView("adult")
    print(df.selectExpr("INT(hash(age)) + INT(hash(age))", "INT(hash(1)) + INT(hash(1))").queryExecution.debug.codegen())
  }
}
