package com.packt.spark.section3

import com.packt.spark._
import org.apache.spark._
import org.apache.spark.rdd._
import geotrellis.vector._
import com.github.nscala_time.time.Imports._

import com.datastax.spark.connector._

/*
create keyspace spark
  with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

create table spark.violations (
  issue_time timestamp,
  location_x double,
  location_y double,
  state varchar,
  agency varchar,
  fine double,
  description varchar,
  neighborhood varchar,
  area double,
  PRIMARY KEY (neighborhood, issue_time)
) with clustering order by (issue_time ASC);
*/

case class CassandraDAO(
  issueTime: DateTime,
  locationX: Double,
  locationY: Double,
  state: String,
  agency: String,
  fine: Double,
  description: String,
  neighborhood: String,
  area: Double
)

object CassandraDAO {
  def from(tup: (NeighborhoodData, Violation)): CassandraDAO = {
    val (neighborhood, violation) = tup
    CassandraDAO(
      violation.issueTime,
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

  def toTuple(dao: CassandraDAO): (NeighborhoodData, Violation) =
    (NeighborhoodData(dao.neighborhood, dao.area),
      Violation(
        dao.issueTime,
        Point(dao.locationX, dao.locationY),
        dao.state,
        dao.agency,
        Ticket(dao.fine, dao.description)
      )
    )
}

object Cassandra extends ExampleApp {
  def run() =
    withSparkContext { implicit sc =>
//      write()
      read()
    }

  def write()(implicit sc: SparkContext): Unit = {
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

      neighborhoodsToViolations
        .map(CassandraDAO.from(_))
        .saveToCassandra("spark", "violations")
  }

  def read()(implicit sc: SparkContext): Unit = {
    val partitionCount = sc.cassandraTable[CassandraDAO]("spark", "violations").partitions.size
    println(s"There are $partitionCount partitions.")
    val readRDD: RDD[(NeighborhoodData, Violation)] =
      sc.cassandraTable[CassandraDAO]("spark", "violations")
        .where("neighborhood = ?", "POINT_BREEZE")
        .where("issue_time > ?", new DateTime(2014,1,1,0,0))
        .map(CassandraDAO.toTuple(_))

    println(readRDD.count)
  }
}
