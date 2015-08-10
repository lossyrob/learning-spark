package com.packt.spark.section2

import com.packt.spark._
import org.apache.spark._
import org.apache.spark.rdd._

object AnatomyOfAnRDD extends ExampleApp {
  def run() =
    withSparkContext { implicit sc =>
      val violations: RDD[Violation] =
        fullDataset
          .mapPartitions { partition =>
            val parse = Violation.rowParser
            partition.flatMap(parse(_))
          }
          .filter(_.ticket.fine > 5.0)

      val count = violations.count
      println(s"Count is $count")
      waitForUser()
    }
}
