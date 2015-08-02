package com.packt.spark.section2

import com.packt.spark._
import org.apache.spark._

object BroadcastVariables extends ExampleApp {
  def run() =
    withSparkContext { implicit sc =>
      val neighborhoods = Neighborhoods.fromJson("data/Neighborhoods_Philadelphia.geojson")
      val bcNeighborhoods = sc.broadcast(neighborhoods)

      val neighborhoodViolations =
        fullDataset
          .mapPartitions { rows =>
            val parse = Violation.rowParser
            rows.flatMap { row => parse(row) }
          }
          .filter(_.ticket.fine > 5.0)
          .flatMap { violationEntry =>
             val nbh = bcNeighborhoods.value
//             val nbh = neighborhoods
             nbh
              .find(_.geom.contains(violationEntry.location)) // Be explicit about this at first, with type
              .map(_.data.name)
           }

      val neighborhoodCounts =
        neighborhoodViolations
          .countByValue

      for((name, count) <- neighborhoodCounts.toSeq.sortBy(_._2)) {
        println(f"\t\t$name%-20s   $count%20d")
      }

      waitForUser()
    }
}
