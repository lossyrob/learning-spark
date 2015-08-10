package com.packt.spark.section2

import com.packt.spark._
import org.apache.spark._
import com.github.nscala_time.time.Imports._

object Serialization extends ExampleApp {
  class TimeDelta(start: DateTime) {
    def apply(other: DateTime): Long =
      start.getMillis - other.getMillis
  }

  def run() =
    withSparkContext { implicit sc =>
      val bcTimeDelta = sc.broadcast(new TimeDelta(new DateTime(2005, 1, 1, 0, 0, 0)))

      val maxTimeDelta =
        fullDataset
          .mapPartitions { rows =>
            val parse = Violation.rowParser
            rows.flatMap { row => parse(row) }
          }
          .filter(_.ticket.fine > 5.0)
          .map { v => bcTimeDelta.value(v.issueTime) }
          .max

      println(s"maxTimeDelta: $maxTimeDelta")
    }
}
