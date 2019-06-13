package com.enzobnl.sparkscalaexpe.playground

import com.enzobnl.sparkscalaexpe.util.QuickSparkSessionFactory
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._

object SparkStuff3 extends Runnable {
  override def run: Unit={
    val spark = QuickSparkSessionFactory.getOrCreate("codeGenExpe")
    val f: Int => Int = (t: Int) => t*2
    val myUDF = udf(f)
    spark.udf.register("myUDF", myUDF)
    import spark.implicits._
    val data = spark.read.format("csv").load("pom.xml")
    val df: DataFrame = data.toDF("a").selectExpr("myUDF(a)", "myUDF(a)")
//    df.show()
    print(df.queryExecution.debug.codegen())
    print(df.explain(true))
    val ds: Dataset[(Int, Int)] = data.asInstanceOf[Dataset[(Int, Int)]]
    val ds2: Dataset[String] = ds.map(_._1.toString + "blabla")
//    ds2.show()
    print(ds2.queryExecution.debug.codegen())
    print(ds2.explain(true))
    System.exit(0)
  }
}
