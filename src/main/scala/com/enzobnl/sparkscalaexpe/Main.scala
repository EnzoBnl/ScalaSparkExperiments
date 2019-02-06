package com.enzobnl.sparkscalaexpe

import com.enzobnl.sparkscalaexpe.playground._

object Main {
  val torun: Runnable = TourOfScalaStuff1

  def main(args: Array[String]): Unit = {
    torun.run
  }
}
