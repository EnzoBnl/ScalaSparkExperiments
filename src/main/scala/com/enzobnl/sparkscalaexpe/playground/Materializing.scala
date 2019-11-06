package com.enzobnl.sparkscalaexpe.playground

import com.enzobnl.sparkscalaexpe.playground.Sb3.DAMPING_FACTOR
import com.enzobnl.sparkscalaexpe.util.Utils
import org.apache.spark.HashPartitioner
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SparkUtils.withNumShufflePartition
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession, SparkUtils}
import org.graphframes.GraphFrame
import org.graphframes.lib.AggregateMessages


object Materializing extends Runnable {
  lazy val spark: SparkSession = SparkSession
    .builder
    .config("spark.default.parallelism", (Runtime.getRuntime().availableProcessors() * 4).toString)
    .config("spark.sql.shuffle.partitions", Runtime.getRuntime().availableProcessors() * 4)
    .master("local[*]")
    .appName("OnCrawlInRankBenches")
    .getOrCreate
  spark.sparkContext.setLogLevel("ERROR")

  def bench() = {
    withNumShufflePartition(spark, 40) {
      val N = 50000000


      spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)



      println((1 to 10).map[Double, Seq[Double]](i => {
        val df1 = spark.range(N).withColumn("data", col("id").cast(StringType) + lit("blaaaaa39UHI")).withColumn("id", pmod(col("id"), lit(100))).cache()
        df1.count()
        val df2 = spark.range(100).cache()
        df2.count()
        val join = df1.join(df2, df1("id") === df2("id"), "left_outer").select(df1("id"))

        println(i)

        val runtime: Double = Utils.getTime {
          join.cache().foreachPartition(p => p.size)  // 52403
//          join.cache().foreachPartition(p => ())  // 46558
//          join.cache().queryExecution.toRdd.foreachPartition(p => p.size) // 49748, 49279
//          join.cache().queryExecution.toRdd.foreachPartition(p => ()) // 47758
//          join.cache().count()  // 51234
        }
        spark.catalog.clearCache()
        runtime
      }).sum)


    }
  }

  def expeMaterialization() = {
    withNumShufflePartition(spark, 40) {
      val N = 50000000


      {
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

        {
          val df1 = spark.range(N).withColumn("data", col("id").cast(StringType) + lit("blaaaaa39UHI")).withColumn("id", pmod(col("id"), lit(100))).cache()
          df1.count()
          val df2 = spark.range(100).cache()
          df2.count()
          val join = df1.join(df2, df1("id") === df2("id"), "left_outer").select(df1("id"))

          println("join.count() solo")

          Utils.time {
            join.count()
          }
          spark.catalog.clearCache()
        }
        {
          val df1 = spark.range(N).withColumn("id", pmod(col("id"), lit(100))).cache()
          df1.count()
          val df2 = spark.range(100).cache()
          df2.count()
          val join = df1.join(df2, df1("id") === df2("id"), "left_outer").select(df1("id"))
          println("join.cache().foreachPartition(p => ())")
          Utils.time {
            join.cache().foreachPartition(p => ())
          }
          Utils.time {
            //          join.count()
            join.groupBy(df1("id")).avg().collect()
          }
          spark.catalog.clearCache()
        }

        {
          val df1 = spark.range(N).withColumn("id", pmod(col("id"), lit(100))).cache()
          df1.count()
          val df2 = spark.range(100).cache()
          df2.count()
          val join = df1.join(df2, df1("id") === df2("id"), "left_outer").select(df1("id"))
          println("join.cache().foreachPartition(p => p.size)")
          Utils.time {
            join.cache().foreachPartition(p => p.size)
          }
          Utils.time {
            //          join.count()
            join.groupBy(df1("id")).avg().collect()
          }
          spark.catalog.clearCache()
        }
        {
          val df1 = spark.range(N).withColumn("id", pmod(col("id"), lit(100))).cache()
          df1.count()
          val df2 = spark.range(100).cache()
          df2.count()
          val join = df1.join(df2, df1("id") === df2("id"), "left_outer").select(df1("id"))
          println("join.cache().queryExecution.toRdd.foreachPartition(p => p.size)")
          Utils.time {
            join.cache().queryExecution.toRdd.foreachPartition(p => p.size)
          }
          Utils.time {
            //          join.count()
            join.groupBy(df1("id")).avg().collect()
          }
          spark.catalog.clearCache()
        }

        {
          val df1 = spark.range(N).withColumn("id", pmod(col("id"), lit(100))).cache()
          df1.count()
          val df2 = spark.range(100).cache()
          df2.count()
          val join = df1.join(df2, df1("id") === df2("id"), "left_outer").select(df1("id"))
          println("join.cache().queryExecution.toRdd.foreachPartition(p => ())")
          Utils.time {
            join.cache().queryExecution.toRdd.foreachPartition(p => ())
          }
          Utils.time {
            //          join.count()
            join.groupBy(df1("id")).count().collect()
          }
          spark.catalog.clearCache()

        }

        {

          val df1 = spark.range(N).withColumn("id", pmod(col("id"), lit(100))).cache()
          df1.count()
          val df2 = spark.range(100).cache()
          df2.count()
          val join = df1.join(df2, df1("id") === df2("id"), "left_outer").select(df1("id"))
          println("join.cache().count()")
          Utils.time {
            join.cache().count()
          }
          Utils.time {
            //          join.count()
            join.groupBy(df1("id")).avg().collect()
          }
          spark.catalog.clearCache()
        }


      }
    }
  }

  override def run(): Unit = {
    bench()
    while (true) {
      Thread.sleep(1000)
    }
  }

}
