package com.packt.spark.section2

import com.packt.spark._
import org.apache.spark._
import geotrellis.vector._

object BroadcastVariables extends ExampleApp {
  def run() =
    withSparkContext { implicit sc =>
      val neighborhoods = Neighborhoods.fromJson("data/Neighborhoods_Philadelphia.geojson")
      val bcNeighborhoods = sc.broadcast(neighborhoods)

      val neighborhoodCounts =
        fullDataset
          .mapPartitions { rows =>
            val parse = Violation.rowParser
            rows.flatMap { row => parse(row) }
          }
          .filter(_.ticket.fine > 5.0)
          .flatMap { violation =>
            val nb = bcNeighborhoods.value
            nb.find { case Feature(polygon, _) =>
              polygon.contains(violation.location)
            }.map { case Feature(_, data) =>
              data.name
            }
          }
          .countByValue

      for((name, count) <- neighborhoodCounts.toSeq.sortBy(_._2)) {
        println(s"$name  $count")
      }

      waitForUser
    }
}
