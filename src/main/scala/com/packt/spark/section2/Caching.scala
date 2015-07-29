package com.packt.spark.section2

import com.packt.spark._
import org.apache.spark._
import geotrellis.vector._

import org.apache.spark.storage._
import com.github.nscala_time.time.Imports._

object Caching extends ExampleApp {
  val timeFilters =
    Map[String, DateTime => Int](
      ("allTime", { dt => 0 }),
      ("monthOfYear", { dt => dt.getMonthOfYear }),
      ("dayOfMonth", { dt => dt.getDayOfMonth }),
      ("dayOfYear", { dt => dt.getDayOfYear }),
      ("dayOfWeek", { dt => dt.getDayOfWeek }),
      ("hourOfDay", { dt => dt.getHourOfDay })
    )


  def densityAggregations(neighborhoods: Neighborhoods)(implicit sc: SparkContext) = {
      val bcNeighborhoods = sc.broadcast(neighborhoods)

      val violationsWithNeighborhoods = 
        violations
          .flatMap { violation =>
            bcNeighborhoods.value
              .find(_.geom.contains(violation.location))
              .map { case Feature(_, data) =>
                (violation, data)
              }
           }
          .persist(StorageLevel.MEMORY_AND_DISK)

      timeFilters.map { case (key, groupingFunc) =>
        val neighborhoodViolationDensities =
          violationsWithNeighborhoods
            .map { case (violation, data) =>
              val timeGroup = groupingFunc(violation.issueTime)
              ((data, timeGroup), 1)
             }
            .reduceByKey { (a, b) => a + b }
            .map { case ((NeighborhoodData(name, area), timeGroup), count) =>
              ((name, timeGroup), count / area)
             }
            .collect
            .toMap
        (key, neighborhoodViolationDensities)
      }
  }

  def run() =
    withSparkContext { implicit sc =>
      val neighborhoods = Neighborhoods.fromJson("data/Neighborhoods_Philadelphia.geojson")
      for((timeKey, map) <- densityAggregations(neighborhoods)) {
        println(timeKey)
        map.foreach { case ((neighborhood, timeGroup), density) =>
          println(s"  $neighborhood $timeGroup   $density")
        }
      }

      waitForUser()
    }
}
