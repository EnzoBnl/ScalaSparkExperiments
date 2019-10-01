package com.enzobnl.sparkscalaexpe

import com.enzobnl.sparkscalaexpe.playground._

object Main extends App {
  val runnables: Seq[Runnable] = Seq(DatasetVsDataFrame)
  for(torun <- runnables) torun.run
}
