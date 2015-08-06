package com.packt.spark.section2

import com.packt.spark._
import org.apache.spark._
import org.apache.spark.rdd._
import geotrellis.vector._

import org.apache.spark.storage._
import com.github.nscala_time.time.Imports._

object Caching extends ExampleApp {
  // val timeGroups =
  //   Map[String, DateTime => Int](
  //     ("allTime", { dt => 0 }),
  //     ("monthOfYear", { dt => dt.getMonthOfYear }),
  //     ("dayOfMonth", { dt => dt.getDayOfMonth }),
  //     ("dayOfYear", { dt => dt.getDayOfYear }),
  //     ("dayOfWeek", { dt => dt.getDayOfWeek }),
  //     ("hourOfDay", { dt => dt.getHourOfDay })
  //   )


  // def neighborhoodGroupCounts(neighborhoodViolations: RDD[(NeighborhoodData, Violation)]): RDD[(NeighborhoodData, (String, Int, Int))] = {
  //   val rdds: Seq[RDD[(NeighborhoodData, Map[Int, Int])]] =
  //     for(groupDef <- timeGroups) yield {
  //       val groupType = groupDef._1
  //       val groupingFunc = groupDef._2

  //       neighborhoodViolations
  //         .mapValues { violation =>
  //           groupingFunc(violation.issueTime)
  //         }
  //         .combineByKey(
  //           { groupId: Int => Map(groupId -> 1) },
  //           { (acc: Map[Int, Int], groupId: Int) => acc + (groupId -> (acc.getOrElse(groupId, 0) + 1)) },
  //           { (acc1: Map[Int, Int], acc2: Map[Int, Int]) =>
  //             (acc1.keySet ++ acc2.keySet).map { k => (k, acc1.getOrElse(k, 0) + acc2.getOrElse(k, 0)) }.toMap
  //           }
  //         )
  //         .flatMapValues { groupCountMap =>
  //           groupCountMap.map { case (groupId, count) => (groupType, groupId, count) }
  //         }
  //     }

  //   rdds.reduce(_.union(_))
  // }

  def run() = ???
  //   withSparkContext { implicit sc =>
  //     val neighborhoods = Neighborhoods.fromJson("data/Neighborhoods_Philadelphia.geojson")
  //     val bcNeighborhoods = sc.broadcast(neighborhoods)

  //     val neighborhoodViolations =
  //       fullDataset
  //         .mapPartitions { rows =>
  //           val parse = Violation.rowParser
  //           rows.flatMap { row => parse(row) }
  //         }
  //         .filter(_.ticket.fine > 5.0)
  //         .flatMap { violation =>
  //           val nb = bcNeighborhoods.value
  //           nb.find { case Feature(polygon, _) =>
  //             polygon.contains(violation.location)
  //           }.map { case Feature(_, data) =>
  //             (data, violation)
  //           }
  //         }
  //         .partitionBy(new HashPartitioner(16))
  //         .persist(StorageLevel.MEMORY_AND_DISK)

  //     val groupCounts = neighborhoodGroupCounts(neighborhoodViolations)



  //     waitForUser()
  //   }
}
