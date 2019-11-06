package com.enzobnl.sparkscalaexpe

import com.enzobnl.sparkscalaexpe.playground._

object Main extends App {
  val runnables: Seq[Runnable] = Seq(KeyWordsExtraction)
  for(torun <- runnables) torun.run
}
