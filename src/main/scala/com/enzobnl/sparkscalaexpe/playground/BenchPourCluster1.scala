package com.enzobnl.sparkscalaexpe.playground

import com.enzobnl.sparkscalaexpe.util.{QuickSparkSessionFactory, Utils}
import org.apache.spark.sql.functions._
import com.enzobnl.sparkscalaexpe.util.Utils.time
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import java.math.BigInteger
import java.security.MessageDigest
object BenchPourCluster1 extends Runnable {
  def fullInsight(df: DataFrame, tag: String): Unit ={
        println(s">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>$tag")
        println(df.queryExecution.debug.codegen())
        println(df.explain(true))
  }
  override def run(): Unit={
    val spark: SparkSession = QuickSparkSessionFactory.getOrCreate("codeGenExpe")

    //UDF
    val f: (Int, Int) => Int =
      (ms: Int, res: Int) => {
        val t0 = System.currentTimeMillis()
        while(System.currentTimeMillis() - t0 < ms){
          MessageDigest.getInstance("SHA-256").digest(t0.toString.getBytes("UTF-8"))
        }
        res
      }
    //time {f(250, 1)}; System.exit(0)
    val my_udf = udf(f)
    spark.udf.register("my_udf_name", my_udf)
    val df = spark.read.option("header", value=true).option("delimiter", "\t").option("inferSchema", value=true).csv("C:/Applications/khiops/samples/Adult/Adult.txt")
    val linesNumber = 50
    val ms = 100
    df.limit(linesNumber).createOrReplaceGlobalTempView("adult")
    // NOT OPTI
    time {spark.sql(s"SELECT SUM(my_udf_name($ms, age)), AVG(my_udf_name($ms,age)+1) FROM global_temp.adult GROUP BY age").collect()}
    //OPTI
    time {spark.sql(s"SELECT SUM(t), AVG(t+1) FROM (SELECT *, my_udf_name($ms, age) AS t FROM global_temp.adult) GROUP BY age").collect()}


    //System.exit(0)
    // NOT OPTI
    fullInsight(spark.sql(s"SELECT SUM(my_udf_name($ms, age)), AVG(my_udf_name($ms,age)+1) FROM global_temp.adult GROUP BY age"), tag="+1 (NO)")
    //OPTI
    fullInsight(spark.sql(s"SELECT SUM(t), AVG(t+1) FROM (SELECT *, my_udf_name($ms, age) AS t FROM global_temp.adult) GROUP BY age"), tag="+1 (O)")

    //System.exit(0)
    // NOT OPTI
    fullInsight(spark.sql(s"SELECT SUM(my_udf_name($ms, age)), AVG(my_udf_name($ms,age)) FROM global_temp.adult GROUP BY age"), tag="_ (NO)")
    //OPTI
    fullInsight(spark.sql(s"SELECT SUM(t), AVG(t) FROM (SELECT *, my_udf_name($ms, age) AS t FROM global_temp.adult) GROUP BY age"), tag="_ (O)")
  }
}

//// EXPE 1 basée ! (il faut ajouter une addition dedans pour qu'elle valide:
////non:
//// NOT OPTI
//time {spark.sql(s"SELECT SUM(my_udf_name($ms, age)), AVG(my_udf_name($ms,age)) FROM global_temp.adult GROUP BY age").collect()}
//  //OPTI
//  time {spark.sql(s"SELECT SUM(t), AVG(t) FROM (SELECT *, my_udf_name($ms, age) AS t FROM global_temp.adult) GROUP BY age").collect()}
////oui:
//// NOT OPTI
//time {spark.sql(s"SELECT SUM(my_udf_name($ms, age)), AVG(my_udf_name($ms,age)+1) FROM global_temp.adult GROUP BY age").collect()}
//  //OPTI
//  time {spark.sql(s"SELECT SUM(t), AVG(t+1) FROM (SELECT *, my_udf_name($ms, age) AS t FROM global_temp.adult) GROUP BY age").collect()}

//    //EXPE 8 validée
//    // NOT OPTI
//    time {spark.sql(s"SELECT my_udf_name($ms, age)*2, my_udf_name($ms,age) FROM global_temp.adult").collect()}
//    //OPTI
//    time {spark.sql(s"SELECT t*2, t FROM (SELECT *, my_udf_name($ms, age) AS t FROM global_temp.adult) ").collect()}