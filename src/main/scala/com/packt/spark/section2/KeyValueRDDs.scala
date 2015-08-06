package com.packt.spark.section2

import com.packt.spark._
import org.apache.spark._
import org.apache.spark.rdd._
import geotrellis.vector._

import org.apache.spark.storage._
import com.github.nscala_time.time.Imports._

object KeyValueRDDs extends ExampleApp {
  def meanWithJoin(neighborhoodsToViolations: RDD[(NeighborhoodData, Violation)]): RDD[(NeighborhoodData, Double)] = {
    val sums =
      neighborhoodsToViolations
        .mapValues(_.ticket.fine)
        .reduceByKey(_ + _)

    val counts =
      neighborhoodsToViolations
        .mapValues { v => 1 }
        .reduceByKey(_ + _)

    sums.join(counts)
      .mapValues { case (sum, count) => sum / count }
  }

  def meanWithGroupByKey(neighborhoodsToViolations: RDD[(NeighborhoodData, Violation)]): RDD[(NeighborhoodData, Double)] =
    neighborhoodsToViolations
      .groupByKey
      .mapValues { violations =>
        violations.map(_.ticket.fine).sum / violations.size
      }

  def meanWithCombineByKey(neighborhoodsToViolations: RDD[(NeighborhoodData, Violation)]): RDD[(NeighborhoodData, Double)] =
    neighborhoodsToViolations
      .combineByKey[(Double, Int)](
        { violation: Violation => (violation.ticket.fine, 1) },
        { (acc: (Double, Int), violation: Violation) => (acc._1 + violation.ticket.fine, acc._2 + 1) },
        { (acc1: (Double, Int), acc2: (Double, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2) }
      )
      .mapValues { case (sum, count) => sum / count }

  def run() =
    withSparkContext { implicit sc =>
      val neighborhoods = Neighborhoods.fromJson("data/Neighborhoods_Philadelphia.geojson")
      val bcNeighborhoods = sc.broadcast(neighborhoods)      

      val neighborhoodsToViolations: RDD[(NeighborhoodData, Violation)] =
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
            }.map { case Feature(_, neighborhoodData) =>
              (neighborhoodData, violation)
            }
          }
//          .partitionBy(new HashPartitioner(50))
//          .persist(storage.StorageLevel.MEMORY_AND_DISK)

      val mean1 = meanWithJoin(neighborhoodsToViolations)
      val mean2 = meanWithGroupByKey(neighborhoodsToViolations)
      val mean3 = meanWithCombineByKey(neighborhoodsToViolations)

      mean1.cogroup(mean2, mean3)
        .foreach { case (neighborhood, (m1, m2, m3)) =>
          println(s"${neighborhood.name}      $m1  $m2  $m3")
        }


      waitForUser()
    }
}
