package com.packt.spark.section3

import com.packt.spark._
import org.apache.spark._
import org.apache.spark.rdd._
import geotrellis.vector._

import org.apache.spark.storage._
import com.github.nscala_time.time.Imports._

import java.util

import scala.collection.JavaConverters._
import org.apache.spark.sql.types._

/**
 * An example class to demonstrate UDT in Scala, Java, and Python.
 * @param x x coordinate
 * @param y y coordinate
 */
@SQLUserDefinedType(udt = classOf[PointUDT])
class ExamplePoint(val x: Double, val y: Double) extends Serializable

class PointUDT extends UserDefinedType[Point] {

  override def sqlType: DataType = ArrayType(DoubleType, false)

  override def serialize(obj: Any): Seq[Double] = {
    obj match {
      case p: Point =>
        Seq(p.x, p.y)
    }
  }

  override def deserialize(datum: Any): Point = {
    datum match {
      case values: Seq[_] =>
        val xy = values.asInstanceOf[Seq[Double]]
        assert(xy.length == 2)
        Point(xy(0), xy(1))
      case values: util.ArrayList[_] =>
        val xy = values.asInstanceOf[util.ArrayList[Double]].asScala
        Point(xy(0), xy(1))
    }
  }

  override def userClass: Class[Point] = classOf[Point]

  override def asNullable: PointUDT = this
}

case class ViolationCount(neighborhood: String, time: java.sql.Timestamp, violationCount: Int)

object Main extends ExampleApp {
  def run() =
    withSparkContext { implicit sc =>
      val neighborhoods = Neighborhoods.fromJson("data/Neighborhoods_Philadelphia.geojson")
      val bcNeighborhoods = sc.broadcast(neighborhoods)      

      val neighborhoodsToViolations: RDD[(PolygonFeature[NeighborhoodData], Violation)] =
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
            }.map { feature =>
              (feature, violation)
            }
          }

      val violationCounts = 
        neighborhoodsToViolations
          .map { case (neighborhood, violation) =>
            val dt = violation.issueTime
            ((neighborhood, new DateTime(dt.getYear, dt.getMonthOfYear, dt.getDayOfMonth, dt.getHourOfDay, 0, 0)), 1)
          }
          .reduceByKey(_ + _)
          .map { case ((neighborhood, time), count) =>
            ViolationCount(neighborhood.data.name, new java.sql.Timestamp(time.getMillis()), count)
        }

      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      import sqlContext.implicits._

      val df = violationCounts.toDF()
      df.registerTempTable("violationcounts")

      val nbs = sqlContext.sql("SELECT COUNT(neighborhood) FROM violationcounts WHERE violationCount > 5")
      nbs.collect().foreach(println)
    }
}
