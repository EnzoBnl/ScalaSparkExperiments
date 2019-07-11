package com.enzobnl.sparkscalaexpe.playground

import com.enzobnl.sparkscalaexpe.playground.Sandbox.spark
import com.enzobnl.sparkscalaexpe.util.QuickSparkSessionFactory
import org.apache.spark.sql.SparkSession
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
    val cfg = "/Users/luca/java/apache-ignite-fabric-2.4.0-bin/config/default-config.xml"

//
//    df.printSchema()
//    df.write
//      .format(FORMAT_IGNITE)
//      .option(OPTION_CONFIG_FILE, cfg)
//      .option(OPTION_TABLE, "PERSON")
//      .option(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "uuid")
//      .mode(SaveMode.Append)
//      .save()


  }
}
