package com.enzobnl.sparkscalaexpe.playground

import java.util

import com.enzobnl.sparkscalaexpe.util.QuickSparkSessionFactory
import org.apache.spark.ml.feature.SQLTransformer
import org.apache.spark.graphx.GraphLoader

import scala.util.Random
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

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


object Sandbox extends Runnable {

  lazy val spark: SparkSession = QuickSparkSessionFactory.getOrCreate()
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
  object Memoizer {
    def memo[R, A1](function: A1 => R): A1 => R = new Memoizer[R, A1, Any]().doMemoize(function)
    def memo[R, A1, A2](function: (A1, A2) => R): (A1, A2) => R = new Memoizer[R, A1, A2]().doMemoize(function)
  }

  class Memoizer[R, A1, A2] {
    lazy val cache = {println("cache created"); scala.collection.mutable.Map[Int, R]()}

    private def doMemoize(function: A1 => R): A1 => R = {
      input => cache.getOrElseUpdate(input.hashCode(), {println("Computed"); function(input)})
    }
    private def doMemoize(function: (A1, A2) => R): (A1, A2) => R = {
      (a1: A1, a2: A2)  => {
        cache.getOrElseUpdate(a1.hashCode() + a2.hashCode(), {println("Computed"); function(a1, a2)})
      }
    }
  }

  override def run(): Unit = {
    import Memoizer.memo
    val f = (i: Int, s: String)=> s.substring(i, i+1)
    val g = (i: Int, s: String)=> s.substring(i-1, i)
    val m = memo(f.tupled)
    spark.udf.register("memo", memo(f))
    df.selectExpr("memo(1, category)").show()
//    import org.apache.spark.sql.functions.udf
//    df.withColumn("computed", udf(g).apply(col("un"), col("category"))).show()
  }
}
