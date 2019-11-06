package org.apache.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.types.StructType

object SparkUtils {
  def DatasetofRows(spark: SparkSession, schema: StructType, rdd: RDD[InternalRow]) = {
    Dataset.ofRows(spark, LogicalRDD(schema.toAttributes, rdd)(spark))
  }

  def withNumShufflePartition[A](spark: SparkSession, n: Int)(block: => A): A = {
    val previous = (
      spark.sessionState.conf.numShufflePartitions,
      spark.sparkContext.conf.get("spark.default.parallelism")
    )
    spark.conf.set("spark.sql.shuffle.partitions", n)
    spark.sparkContext.conf.set("spark.default.parallelism", n.toString)
    val res = block
    spark.conf.set("spark.sql.shuffle.partitions", previous._1)
    spark.sparkContext.conf.set("spark.default.parallelism", previous._2)
    res
  }
}
