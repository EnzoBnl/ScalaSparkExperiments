package com.enzobnl.sparkscalaexpe.playground

import java.util

import com.enzobnl.sparkscalaexpe.util.QuickSparkSessionFactory
import org.apache.spark.ml.feature.SQLTransformer

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

object Sandbox extends Runnable {
  lazy val spark: SparkSession = QuickSparkSessionFactory.getOrCreate()
  val df = spark.createDataFrame(
    Seq(("Thin", "Cell", 6000),
      ("Normal", "Tablet", 1500),
      ("Mini", "Tablet", 5500),
      ("Ultra thin", "Cell", 5000),
      ("Very thin", "Cell", 6000),
      ("Big", "Tablet", 2500),
      ("Bendable", "Cell", 3000),
      ("Foldable", "Cell", 3000),
      ("Pro", "Tablet", 4500),
      ("Pro2", "Tablet", 6500))).toDF("product", "category", "revenue")

  override def run(): Unit = {
    def f(i: Int): Unit ={
      //i=2
    }
  }
}
