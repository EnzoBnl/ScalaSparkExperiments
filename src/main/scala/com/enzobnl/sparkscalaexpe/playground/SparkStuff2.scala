package com.enzobnl.sparkscalaexpe.playground

import com.enzobnl.sparkscalaexpe.util.QuickSQLContextFactory
import org.apache.spark.sql.functions._

object SparkStuff2 extends Runnable {
  override def run: Unit={
    val spark = QuickSQLContextFactory.getOrCreate("codeGenExpe")

    //UDF
    val my_udf = udf((i: Int, j: Int) => {i + j})
    spark.udf.register("my_udf_name", my_udf)
    val my_udf2 = udf((i: Int, j: Int) => {var j=0;for(k <- 1 to 1000000){if(k%3==0)j=j+k;};j})
    spark.udf.register("my_udf_name2", my_udf2)
    val df = spark.read.option("header", true).option("delimiter", "\t").option("inferSchema", true).csv("C:/Applications/khiops/samples/Adult/Adult.txt")
    df.createOrReplaceGlobalTempView("adult")
    //print(df.selectExpr("INT(hash(age)) + INT(hash(age))", "INT(hash(1)) + INT(hash(1))").queryExecution.debug.codegen())
    //print(spark.sql("SELECT INT(hash(race)) + INT(hash(age)), hash(race) FROM global_temp.adult").queryExecution.debug.codegen())
    //print(spark.sql("SELECT (exp(INT(race)) + exp(INT(race)))*(exp(INT(race)) + exp(INT(race))) FROM global_temp.adult").queryExecution.debug.codegen())
    // covar: Optimized (aggregate with 2 args)
    //    val df_ = spark.sql("SELECT concat(age, INT(race)) + concat(age, INT(race)), concat(age, INT(race)) FROM global_temp.adult") // NO
    val df_ = spark.sql("SELECT my_udf_name(age, education_num) + my_udf_name(age, education_num), my_udf_name(age, education_num) FROM global_temp.adult")
    //val df_ = spark.sql("SELECT hash(age+ INT(race)) + hash(age+ INT(race)), hash(age, INT(race)) FROM global_temp.adult")
    val df__ = spark.sql("SELECT my_udf_name2(age, education_num) + my_udf_name2(age, education_num), my_udf_name2(age, education_num) FROM global_temp.adult")

    print(df_.queryExecution.debug.codegen())
    print(df_.explain(true))
    print(df__.queryExecution.debug.codegen())
    print(df__.explain(true))

  }
}
