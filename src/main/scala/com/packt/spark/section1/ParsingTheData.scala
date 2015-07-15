package com.packt.spark.section1

import com.packt.spark._

import org.apache.spark._
import org.apache.spark.rdd._

object ParsingTheData extends ExampleApp {
  def run() =
    withSparkContext { implicit sc =>
      val parsed: RDD[Option[Violation]] = 
        sampleDataset
          .map(Violation.fromRow _)

      val count = parsed.count

      println(f"\nCount is ${count}%,d.\n")

      waitForUser()
    }
}
