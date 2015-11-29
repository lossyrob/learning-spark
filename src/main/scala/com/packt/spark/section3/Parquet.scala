package com.packt.spark.section3

import com.packt.spark._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import geotrellis.vector._
import com.github.nscala_time.time.Imports._
import java.sql.Timestamp

case class ParquetDAO(
  issueTime: Timestamp,
  locationX: Double,
  locationY: Double,
  state: String,
  agency: String,
  fine: Double,
  description: String,
  neighborhood: String,
  area: Double
) {
  def toTuple(): (NeighborhoodData, Violation) =
    (NeighborhoodData(neighborhood, area),
      Violation(
        new DateTime(issueTime),
        Point(locationX, locationY),
        state,
        agency,
        Ticket(fine, description)
      )
    )
}

object ParquetDAO {
  def from(tup: (NeighborhoodData, Violation)): ParquetDAO = {
    val (neighborhood, violation) = tup
    ParquetDAO(
      new Timestamp(violation.issueTime.getMillis()),
      violation.location.x,
      violation.location.y,
      violation.state,
      violation.agency,
      violation.ticket.fine,
      violation.ticket.description,
      neighborhood.name,
      neighborhood.area
    )
  }
}


object Parquet extends ExampleApp {
  def run() =
    withSparkContext { implicit sc =>
      val path = "/Users/rob/proj/packt/spark/test-data/violations.parquet"
//      val path = "hdfs://localhost/test/TEST-OUT-SPARK-HDFS-PARQUET"
//      write(path)
//      readRDD(path)
      readSQL(path)
    }

  def write(path: String)(implicit sc: SparkContext): Unit = {
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
        }.map { case Feature(_, neighborhood) =>
            (neighborhood, violation)
        }
      }

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    neighborhoodsToViolations
      .map(ParquetDAO.from(_))
      .toDF()
      .write.parquet(path)
  }

  def readRDD(path: String)(implicit sc: SparkContext): Unit = {
    val sqlContext = new SQLContext(sc)

    val readRDD: RDD[(NeighborhoodData, Violation)] =
      sqlContext.read.parquet(path)
        .map { row =>
          row match {
            case Row(issueTime: Timestamp,
              locationX: Double,
              locationY: Double,
              state: String,
              agency: String,
              fine: Double,
              description: String,
              neighborhood: String,
              area: Double
            ) => ParquetDAO(issueTime, locationX, locationY, state, agency, fine, description, neighborhood, area).toTuple
            case _ => sys.error("Violation expected")
          }
      }

    println(s"\n\n${readRDD.count}\n\n")
  }

  def readSQL(path: String)(implicit sc: SparkContext): Unit = {
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.parquet(path)
    df.registerTempTable("violations")

    val count = sqlContext.sql("SELECT * FROM violations WHERE neighborhood = 'POINT_BREEZE' AND issueTime > '2012-07-16'").count

    println(s"\n\n${count}\n\n")
  }
}
