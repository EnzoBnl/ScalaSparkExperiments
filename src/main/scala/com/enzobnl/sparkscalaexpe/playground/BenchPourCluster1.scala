package com.enzobnl.sparkscalaexpe.playground

import com.enzobnl.sparkscalaexpe.util.QuickSQLContextFactory
import org.apache.spark.sql.functions._

object BenchPourCluster1 extends Runnable {
  override def run: Unit={
    val spark = QuickSQLContextFactory.getOrCreate("codeGenExpe")

    //UDF
    val my_udf = udf((i: Int, j: Int) => i + j)
    spark.udf.register("my_udf_name", my_udf)
    val df = spark.read.option("header", true).option("delimiter", "\t").option("inferSchema", true).csv("C:/Applications/khiops/samples/Adult/Adult.txt")
    df.createOrReplaceGlobalTempView("adult")
    //print(df.selectExpr("INT(hash(age)) + INT(hash(age))", "INT(hash(1)) + INT(hash(1))").queryExecution.debug.codegen())
    //print(spark.sql("SELECT INT(hash(race)) + INT(hash(age)), hash(race) FROM global_temp.adult").queryExecution.debug.codegen())
    //print(spark.sql("SELECT (exp(INT(race)) + exp(INT(race)))*(exp(INT(race)) + exp(INT(race))) FROM global_temp.adult").queryExecution.debug.codegen())
    // covar: Optimized (aggregate with 2 args)
    //    val df_ = spark.sql("SELECT concat(age, INT(race)) + concat(age, INT(race)), concat(age, INT(race)) FROM global_temp.adult") // NO
    val df_ = spark.sql("SELECT my_udf_name(age, education_num) + my_udf_name(age, education_num), my_udf_name(age, education_num) FROM global_temp.adult")
    //val df_ = spark.sql("SELECT hash(age+ INT(race)) + hash(age+ INT(race)), hash(age, INT(race)) FROM global_temp.adult")

    print(df_.queryExecution.debug.codegen())
    print(df_.explain(true))

  }
}
