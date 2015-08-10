package com.packt.spark.section1

import com.packt.spark._
import org.apache.spark._

object ParsingTheData extends ExampleApp {
  def run() =
    withSparkContext { implicit sc =>
      val rdd =
        fullDataset
          .map(Violation.fromRow _)

      val count = rdd.count
      println(f"\nCount is ${count}%,d\n")

      waitForUser()
    }
}
