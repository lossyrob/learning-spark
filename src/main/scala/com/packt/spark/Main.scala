package com.packt.spark

import scala.io._

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import geotrellis.vector._
import geotrellis.vector.io.json._
import spray.json._
import com.github.nscala_time.time.Imports._

case class NeighborhoodData(name: String, listName: String)

object Main {
  def getLocalSparkContext(): SparkContext = {
    val conf = 
      new SparkConf()
        .setMaster("local[4]")
        .setAppName("Discover PPA")
    new SparkContext(conf)
  }

  // For doing spark submit
  def getSparkContext(): SparkContext = {
    val conf = 
      new SparkConf()
        .setAppName("Discover PPA")
    new SparkContext(conf)
  }

  def main(args: Array[String]): Unit = {
    val sc = getLocalSparkContext

    // Introducing the Spark UI
    println("Press enter to continue..." )
    Console.readLine

//    val path = "data/Parking_Violations-sample.csv"
    val path = "file:/Users/rob/proj/packt/spark/discover-ppa/data/Parking_Violations.csv"

    // Learning accumulators

    val parsed = 
      sc.textFile(path)
        .filter(!_.startsWith("Issue"))
        .map(Violation.fromRow _)

    val fullCount = parsed.count
    println(s"Original line count: $fullCount")

    val violationEntries =
      parsed
        .filter(_.isDefined)
        .map { option => 
          val entry = option.get
          entry
        }
//        .flatMap { x => x }

    val count = violationEntries.count
    println(s"Count is $count.")

    // Actions and Transformations
    // violationEntries
    //   .map { ve =>
    //     ve
    //    }
    //   .reduce { case ((d1, c1), (d2, c2)) =>



    // Learning Broadcast variables
    val neighborhoods = Neighborhoods.fromJson("data/Neighborhoods_Philadelphia.geojson")
    val bcNeighborhoods = sc.broadcast(neighborhoods)

    val neighborhoodViolations = 
      violationEntries.map { violationEntry =>
        bcNeighborhoods.value.find(_.geom.contains(violationEntry.location)) match {
          case Some(mpf) =>
            (violationEntry.issueDate, violationEntry.location, mpf.data)
          case None =>
            (violationEntry.issueDate, violationEntry.location, "UNKNOWN")
          }
        }

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
  }
}
