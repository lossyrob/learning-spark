package com.packt.spark.section2

import com.packt.spark._
import org.apache.spark._

import geotrellis.vector._
import geotrellis.vector.io.json._

object AnatomyOfAnRDD extends ExampleApp {
  def run() =
    withSparkContext { implicit sc =>
      val centroid =
        fullDataset
          .flatMap(Violation.fromRow _)
          .map(_.location)
          .mapPartitions { partition =>
            Seq(partition.toSeq.centroid).iterator
           }
          .collect
          .flatMap(_.as[Point])
          .centroid.as[Point].get

      println(s"Centroid: ${centroid.toGeoJson}.")
      waitForUser()
    }
}
