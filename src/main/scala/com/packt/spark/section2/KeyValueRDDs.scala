package com.packt.spark.section2

import com.packt.spark._
import org.apache.spark._
import geotrellis.vector._

object KeyValueRDDs extends ExampleApp {
  def run() =
    withSparkContext { implicit sc =>
      val neighborhoods = Neighborhoods.fromJson("data/Neighborhoods_Philadelphia.geojson")
      val bcNeighborhoods = sc.broadcast(neighborhoods)

      val neighborhoodViolationDensities =
        fullDataset
          .flatMap(Violation.fromRow _)
          .flatMap { violationEntry =>
            val nbh = bcNeighborhoods.value
            nbh
              .find(_.geom.contains(violationEntry.location))
              .map { case Feature(_, data) =>
                (data, 1)
               }
           }
          .reduceByKey { (a, b) => a + b }
          .map { case (NeighborhoodData(name, area), count) => 
            (name, count / area) 
           }
          .collect
          .sortBy(_._2)

      for((name, density) <- neighborhoodViolationDensities) {
        println(s"$name   $density")
      }

      waitForUser()
    }
}
    // Get the sum of fines per neighborhood

    // Learning Key Value Pair RDD
    // val neighborhoodCounts =
    //   neighborhoodViolations
    //     .groupBy { case (_, _, neighborhood) => neighborhood }
    //     .combineByKey(
    //       { violations: Iterable[(DateTime, Point, String)] => violations.size },
    //       { (count: Int, violations: Iterable[(DateTime, Point, String)]) => count + violations.size },
    //       { (count1: Int, count2: Int) => count1 + count2 }
    //      )
    //     .collect

    // for( (neighborhood, count) <- neighborhoodCounts) {
    //   println(s"$neighborhood   $count")
    // }
