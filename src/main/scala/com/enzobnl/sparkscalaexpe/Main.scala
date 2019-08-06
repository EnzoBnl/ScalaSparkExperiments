package com.enzobnl.sparkscalaexpe

import com.enzobnl.sparkscalaexpe.playground._

object Main extends App {
  val runnables: Seq[Runnable] = Seq(IgniteSb2)
  for(torun <- runnables) torun.run
}
