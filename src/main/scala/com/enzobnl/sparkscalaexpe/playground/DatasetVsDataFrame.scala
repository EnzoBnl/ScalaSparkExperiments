package com.enzobnl.sparkscalaexpe.playground

import com.enzobnl.sparkscalaexpe.util.{QuickSparkSessionFactory, Utils}
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Encoders, Row, SparkSession}

case class User(id: Int, pseudo: String, name: String)
object DatasetVsDataFrame extends Runnable {

  lazy val spark: SparkSession = QuickSparkSessionFactory.getOrCreate()

  override def run(): Unit = {
    val sc = spark.sparkContext
    // Load the edges as a graph
    val sparkPath = "/home/enzo/Prog/spark/"
    println("# DATAFRAME")
    val df = spark.read
      .format("csv")
      .option("header", "false")
      .option("delimiter", ",")
      .schema(StructType(Seq(
        StructField("id", IntegerType, true),
        StructField("pseudo", StringType, true),
        StructField("name", StringType, true))))
//      .option("inferSchema", "true")
      .load("/home/enzo/Data/sofia-air-quality-dataset/2019-05_bme280sof.csv")
      .toDF("id", "pseudo", "name")
      .selectExpr("*", "substr(pseudo, 2) AS sub")
      .filter("sub LIKE 'a%' ")
//    df.show()
    df.queryExecution.debug.codegen()
    Utils.time {val _0: RDD[Row] = df.rdd}
    Utils.time {val _1: Array[Row] = df.rdd.collect()}
      Utils.time {val _2: RDD[InternalRow] = df.queryExecution.toRdd}
        Utils.time {val _3: Array[InternalRow] = df.queryExecution.toRdd.collect()}

    println("# DATASET")

    import spark.implicits._
    val ds = spark.read
      .format("csv")
      .option("header", "false")
      .option("delimiter", ",")
      .schema(StructType(Seq(
        StructField("id", IntegerType, true),
        StructField("pseudo", StringType, true),
        StructField("name", StringType, true))))
      //      .option("inferSchema", "true")
      .load("/home/enzo/Data/sofia-air-quality-dataset/2019-05_bme280sof.csv")
      //      .load(sparkPath + "data/graphx/users.txt")
      .toDF("id", "pseudo", "name")
      .as[User]
      .map((user: User) => if(user.name != null)(user.id, user.name, user.pseudo, user.name.substring(1)) else (user.id, user.name, user.pseudo, ""))
      .filter((extendedUser: (Int, String, String, String)) => extendedUser._4.startsWith("a"))
//    ds.show()
    ds.queryExecution.debug.codegen()

    Utils.time {val __0: RDD[(Int, String, String, String)] = ds.rdd}
    Utils.time {val __1: Array[(Int, String, String, String)] = ds.rdd.collect()}
    Utils.time {val __2: RDD[InternalRow] = ds.queryExecution.toRdd}
    Utils.time {val __3: Array[InternalRow] = ds.queryExecution.toRdd.collect()}
  }
}
