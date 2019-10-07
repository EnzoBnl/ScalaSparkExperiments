package com.enzobnl.sparkscalaexpe

import com.enzobnl.sparkscalaexpe.playground._
import org.apache.spark.sql.GraphFramesPageRank

object Main extends App {
  val runnables: Seq[Runnable] = Seq(GraphFramesPageRank)
  for(torun <- runnables) torun.run
}
