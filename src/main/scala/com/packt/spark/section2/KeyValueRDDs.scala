package com.packt.spark.section2

import com.packt.spark._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.storage._
import geotrellis.vector._

object KeyValueRDDs extends ExampleApp {
  def meanWithGroupByKey(neighborhoodsToViolations: RDD[(NeighborhoodData, Violation)]): RDD[(NeighborhoodData, Double)] =
    neighborhoodsToViolations
      .groupByKey
      .mapValues { violations =>
        val (sum, count) =
          violations.foldLeft((0.0, 0)) { (acc, violation) =>
            (acc._1 + violation.ticket.fine, acc._2 + 1)
          }

        sum / count
     }

  def meanWithCombineByKey(neighborhoodsToViolations: RDD[(NeighborhoodData, Violation)]) =
    neighborhoodsToViolations
      .combineByKey(
        { (violation: Violation) => (violation.ticket.fine, 1) },
        { (acc: (Double, Int), violation: Violation) => (acc._1 + violation.ticket.fine, acc._2 + 1) },
        { (acc1: (Double, Int), acc2: (Double, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2) }
      )
      .mapValues { case (sum, count) => sum / count }

  def meanWithJoin(neighborhoodsToViolations: RDD[(NeighborhoodData, Violation)]) = {
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
            val nbs = bcNeighborhoods.value
            nbs.find { case Feature(polygon, _) =>
              polygon.contains(violation.location)
            }.map { case Feature(_, neighborhood) =>
              (neighborhood, violation)
            }
          }
          .partitionBy(new HashPartitioner(48))
          .persist(StorageLevel.MEMORY_AND_DISK)

      val mean1 = meanWithGroupByKey(neighborhoodsToViolations)
      val mean2 = meanWithCombineByKey(neighborhoodsToViolations)
      val mean3 = meanWithJoin(neighborhoodsToViolations)

      mean1.cogroup(mean2, mean3)
        .foreach { case (neighborhood, (m1, m2, m3)) =>
          println(s"${neighborhood.name}  $m1  $m2  $m3")
        }

      waitForUser
    }
}
