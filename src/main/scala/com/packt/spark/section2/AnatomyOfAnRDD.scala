package com.packt.spark.section2

import com.packt.spark._
import org.apache.spark._

import geotrellis.vector._
import geotrellis.vector.io.json._

object AnatomyOfAnRDD extends ExampleApp {
  def run() =
    withSparkContext { implicit sc =>
      val count = violations.count
      println(s"Count is $count")
      waitForUser()
    }
}
