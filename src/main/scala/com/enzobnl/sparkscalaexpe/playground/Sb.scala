package com.enzobnl.sparkscalaexpe.playground

import java.sql.Date

import cats.Functor
import com.codahale.metrics.Metric
import com.enzobnl.sparkscalaexpe.playground.Sandbox.spark
import com.enzobnl.sparkscalaexpe.util.{QuickSparkSessionFactory, Utils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

/* UNION TYPE
//    type ¬[A] = A => Nothing
//    val x: ¬[Int] = (i: Int) => ???
//    type ∨[A, B] = ¬[¬[A] with ¬[B]]
//    type ¬¬[A] = ¬[¬[[A]]
//    type |∨|[A, B] = { type λ[X] = ¬¬[X] <:< (A ∨ B) }
//    def size[T : (Int |∨| String)#λ](t : T) = t match {
//      case i : Int => i
//      case s : String => s.length
//    }
//    print(size(1), size("bla"))
 */

//class StorableComputing[R](function: AnyRef, args: Seq[Any], computedResult: R, time){
//  override def hashCode(): Int = function.hashCode() + args.hashCode()
//  override def toString: String = s"($a, $b)"
//}

/**
  *
  * @tparam R Return type
  */
//trait MemoCache[R]{
//  def getOrElseCompute(key: Int, block: => R)
//}

//class BasicMemoCache[R] extends MemoCache[R]{
//  var map = new scala.collection.mutable.TreeSet[ComputedValue]()
//  override def getOrElseCompute(key: Int, block: => R): Unit = {
//
//    map.getOrElse(key,
//      {
//        map = map ++ Map(key -> block)
//      })
//  }
//}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
object Sb extends Runnable {
  import org.apache.log4j.Logger
  import org.apache.log4j.Level

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  private lazy val spark = SparkSession.builder()
    .appName("Example Program")
    .master("local[1]")
    .getOrCreate()
  override def run(): Unit = {


    val df = spark.createDataFrame(
      Seq(("Thin", "Cell", 6000, 1),
        ("Normal", "Tablet", 1500, 1),
        ("Mini", "Tablet", 5500, 1),
        ("Ultra thin", "Cell", 5000, 1),
        ("Very thin", "Cell", 6000, 1),
        ("Big", "Tablet", 2500, 2),
        ("Bendable", "Cell", 3000, 2),
        ("Foldable", "Cell", 3000, 2),
        ("Pro", "Tablet", 4500, 2),
        ("Pro2", "Tablet", 6500, 2))).toDF("product", "category", "revenue", "un")
    val df2 = spark.createDataFrame(
      Seq(("Thin", "Cell", 6000, 1),
        ("Normal", "Tablet", 1500, 1),
        ("Mini", "Tablet", 5500, 1),
        ("Ultra thin", "Cell", 5000, 1),
        ("Very thin", "Cell", 6000, 1),
        ("Big", "Tablet", 2500, 2),
        ("Bendable", "Cell", 3000, 2),
        ("Foldable", "Cell", 3000, 2),
        ("Pro", "Tablet", 4500, 2),
        ("Pro2", "Tablet", 6500, 2))).toDF("product", "category", "revenue", "un")

//    bench(df,df2)

for(_<-(1 to 45))
    bench(spark.read.csv("c:/Prog/Data/police-department-incidents.csv").limit(500000),
      spark.read.csv("c:/Prog/Data/police-department-incidents.csv").limit(500000),
      spark.read.csv("c:/Prog/Data/police-department-incidents.csv").limit(500000),
      spark.read.csv("c:/Prog/Data/police-department-incidents.csv").limit(500000))

  }
  def bench(dfs: DataFrame*): Unit = {
    import org.apache.spark.sql.Encoders.{tuple, scalaLong}
    var rdd: Any = None
    //DF
    println("DF")
    Utils.time(rdd = dfs(0).withColumn("i", monotonically_increasing_id()).select("i"))
    Utils.time(println(rdd.asInstanceOf[DataFrame].count()))
    Utils.time(println(rdd.asInstanceOf[DataFrame].selectExpr("i+10","i-10").collect()(0)))
    // DF map
    println("DF map")
    Utils.time(rdd = dfs(1).withColumn("i", monotonically_increasing_id()).select("i"))
    Utils.time(println(rdd.asInstanceOf[DataFrame].count()))
    Utils.time(println(rdd.asInstanceOf[DataFrame]
      .map((row: Row) => (row.getLong(0)+10, row.getLong(0)-10))(tuple(scalaLong, scalaLong))
      .collect()(0)))
    // .rdd
    println("rdd")
    Utils.time(rdd = dfs(2).withColumn("i", monotonically_increasing_id()).rdd)
    Utils.time(println(rdd.asInstanceOf[RDD[Row]].count()))
    Utils.time(println(rdd.asInstanceOf[RDD[Row]]
      .map((row: Row) => Row.fromSeq(Seq(row.getAs[Long]("i")+10, row.getAs[Long]("i")-10)))
      .collect()(0)))
  // .toRdd
    println("toRdd")
    Utils.time(rdd = dfs(3).withColumn("i", monotonically_increasing_id()).select("i").queryExecution.toRdd())
    Utils.time(println(rdd.asInstanceOf[RDD[InternalRow]].count()))
    Utils.time(println(rdd.asInstanceOf[RDD[InternalRow]]
      .map((row: InternalRow) => InternalRow.fromSeq(Seq(row.getLong(0)+10, row.getLong(0)-10)))
      .collect()(0)))


  }
  case class T[F[T]](private val f: F[_]){
    val a = f
    def a(s: String*) = s.toSeq
  }
  new T(1).a(Seq("e","r"): _*)


}

