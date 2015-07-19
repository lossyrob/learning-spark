package com.packt.spark.section1

import com.packt.spark._

import org.apache.spark._
import org.apache.spark.rdd._

object ParsingTheData extends ExampleApp {
  def run() =
    withSparkContext { implicit sc =>
      val count = fullDataset.count

      println(f"\nCount is ${count}%,d.\n")

      waitForUser()
    }
}
