package com.enzobnl.sparkscalaexpe.playground

import com.enzobnl.sparkscalaexpe.util.Utils
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.SparkUtils.withNumShufflePartition
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType


object PartitioningFritech extends Runnable {
  lazy val spark: SparkSession = SparkSession
    .builder
    .config("spark.default.parallelism", (Runtime.getRuntime().availableProcessors() * 4).toString)
    .config("spark.sql.shuffle.partitions", Runtime.getRuntime().availableProcessors() * 4)
    .master("local[*]")
    .appName("OnCrawlInRankBenches")
    .getOrCreate
  spark.sparkContext.setLogLevel("ERROR")

  def expeFritechSkew() = {
    withNumShufflePartition(spark, 40) {
      val N = 10000000

      {
        println(s"$N records of value in range(100)")
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
        val df1 = spark.range(N).withColumn("id", pmod(col("id"), lit(100))).cache()
        df1.count()
        val df2 = spark.range(100).cache()
        df2.count()
        val join = df1.join(df2, df1("id") === df2("id"), "left_outer")
        Utils.time {
          println("count is", join.count())
        }
//        df1.repartition(col("id")).queryExecution.toRdd.mapPartitions(p => Iterator(p.size)).collect().foreach(println)
      }
      {
        println(s"$N records of value 1")
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
        val df1 = spark.range(N).withColumn("id", lit(1)).cache()
        df1.count()
        val df2 = spark.range(100).withColumn("id", lit(1)).cache()
        df2.count()
        val join = df1.join(df2, df1("id") === df2("id"), "left_outer")
        Utils.time {
          println("count is", join.count())
        }
        Utils.time {
          println("count is", join.foreachPartition(p => p.size))
        }
        df1.repartition(col("id")).queryExecution.toRdd.mapPartitions(p => Iterator(p.size)).collect().foreach(println)
      }
      {
        println(s"$N records of value in range(100)")
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
        val df1 = spark.range(N).withColumn("id", pmod(col("id"), lit(100))).cache()
        df1.count()
        val df2 = spark.range(100).cache()
        df2.count()
        val join = df1.join(df2, df1("id") === df2("id"), "left_outer")
        Utils.time {
          println("count is", join.count())
        }
        df1.repartition(col("id")).queryExecution.toRdd.mapPartitions(p => Iterator(p.size)).collect().foreach(println)
      }
    }
  }

def linkedIn2dJoin() = {
//  withNumShufflePartition(spark, 30) {
//    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
//    val df = spark.range(10000).withColumn("id", (col("id") / lit(1000)).cast(IntegerType)) // 10 valeur d'id
//      .withColumn("v", (rand(0) * lit(3)).cast(IntegerType))
//      .repartition(30, col("id"), col("v")).sortWithinPartitions("id")
//
//    val left = spark.range(1000).withColumn("id", (col("id") / lit(100)).cast(IntegerType)) // 10 valeur d'id
//      .repartition(5, col("id")).sortWithinPartitions("id")
//
//    val right = spark.range(1000).withColumn("v", (rand(0) * lit(3)).cast(IntegerType)).drop("id")
//      .repartition(6, col("v"))
//
//    //      df.show()
//    //      left.show()
//    //      right.show()
//    left.join(df, df("id") === left("id"), "right_outer").join(right, df("v") === right("v"), "left_outer").explain()
//  }
//
//  withNumShufflePartition(spark, 30) {
//    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
//    val df = spark.range(10000).withColumn("id", (col("id") / lit(1000)).cast(IntegerType)) // 10 valeur d'id
//      .withColumn("v", (rand(0) * lit(3)).cast(IntegerType))
//    //.repartition(30, col("id"), col("v")).sortWithinPartitions("id")
//
//    val left = spark.range(1000).withColumn("id", (col("id") / lit(100)).cast(IntegerType)) // 10 valeur d'id
//    // .repartition(5, col("id")).sortWithinPartitions("id")
//
//    val right = spark.range(1000).withColumn("v", (rand(0) * lit(3)).cast(IntegerType)).drop("id")
//    // .repartition(6, col("v"))
//
//    //      df.show()
//    //      left.show()
//    //      right.show()
//    left.join(df, df("id") === left("id"), "right_outer").join(right, df("v") === right("v"), "left_outer").explain()
//  }

//  withNumShufflePartition(spark, 30) {
//    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
//    val df = spark.range(10000).withColumn("id", (col("id") / lit(1000)).cast(IntegerType)) // 10 valeur d'id
//      .withColumn("v", (rand(0) * lit(3)).cast(IntegerType))
//    //      .repartition(30, col("id"), col("v")).sortWithinPartitions("id")
//
//    val left = spark.range(1000).withColumn("id", (col("id") / lit(100)).cast(IntegerType)) // 10 valeur d'id
//    //       .repartition(5, col("id")).sortWithinPartitions("id")
//
//    val right = spark.range(1000).withColumn("v", (rand(0) * lit(3)).cast(IntegerType)).drop("id")
//    //       .repartition(6, col("v"))
//
//    val leftrdd = left.rdd.map( r => (r.getAs[Int]("id"), r)).partitionBy(new HashPartitioner(5))
//    val rightrdd = left.rdd.map( r => (r.getAs[Int]("id"), r)).partitionBy(new HashPartitioner(6))
//    val dfrdd = df.rdd.map( r => ((r.getAs[Int]("id"), r.getAs[Int]("v")), r)).partitionBy(new HashPartitioner(30))
//
//    left.join(df, df("id") === left("id"), "right_outer").join(right, df("v") === right("v"), "left_outer").explain()
//  }
  withNumShufflePartition(spark, 30) {
    import spark.implicits._
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    val df = spark.range(10000).withColumn("id", (col("id") / lit(1000)).cast(IntegerType)) // 10 valeur d'id
      .withColumn("v", (rand(0) * lit(3)).cast(IntegerType))
    val left = spark.range(1000).withColumn("id", (col("id") / lit(100)).cast(IntegerType)) // 10 valeur d'id
    val right = spark.range(1000).withColumn("v", (rand(0) * lit(3)).cast(IntegerType)).drop("id")
    val leftrdd: RDD[(Int, Row)] = left.rdd.map( r => (r.getAs[Int]("id"), r)).partitionBy(new HashPartitioner(5))
    val rightrdd = left.rdd.map( r => (r.getAs[Int]("id"), r)).partitionBy(new HashPartitioner(6))
    val dfrdd = df.rdd.map( r => ((r.getAs[Int]("id"), r.getAs[Int]("v")), r)).partitionBy(new HashPartitioner(30))
    df.repartition(struct(col("id"), col("v"))).explain()
    df.repartition(col("id"), col("v")).explain()
    left.repartition(col("id"))
      .join(df.repartition(col("id"), col("v")),
        left("id") === df("id")).explain()
    left.repartition(col("id"))
      .join(df.repartition(struct(col("id"), col("v"))),
        left("id") === df("id")).explain()
    spark.sql("SELECT * FROM global_temps.t")
//    left.join(df, df("id") === left("id"), "right_outer").join(right, df("v") === right("v"), "left_outer").explain()
  }
}

  override def run(): Unit = {
    linkedIn2dJoin()
    while (true) {
      Thread.sleep(1000)
    }
  }

}
