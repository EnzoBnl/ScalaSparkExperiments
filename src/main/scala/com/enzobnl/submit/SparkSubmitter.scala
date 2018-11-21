package com.enzobnl.submit
import sys.process._
object SparkSubmitter {
  def main(args: Array[String]): Unit = {
    val mainClass = "com.enzobnl.wordcountsparkscala.WordCount"
    val jar = "./target/wordcountsparkscala-1.0-SNAPSHOT.jar"
    val in = "pom.xml"
    val out = "output"
    val toCall = s"c:/applications/anaconda3/scripts/spark-submit.cmd --class $mainClass $jar $in $out"
    println(toCall)
    toCall !

  }
}
