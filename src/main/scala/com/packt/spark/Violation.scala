package com.packt.spark

import com.github.nscala_time.time.Imports._
import geotrellis.vector._
import com.opencsv._

case class Violation(
  issueDate: DateTime,
  location: Point,
  ticket: Ticket
)

object Violation {
  private val parser = new CSVParser(',')

  private val coordsRx = """\(([^,]+),([^)]+)\)""".r

  private val violationColumn: Map[String, Int] =
    List(
      "issue_date",
      "state",
      "plate",
      "division",
      "location",
      "location-standaridized",
      "coordinates",
      "description",
      "fine",
      "agency",
      "location"
    ).zipWithIndex.toMap


  def parseTime(s: String): DateTime =
    DateTime.parse(s, DateTimeFormat.forPattern("MM/DD/YYYY HH:mm:ss aa"))

  def parseLocation(l: String): Option[Point] =
    l match {
      case coordsRx(lat, lng) => Some(Point(lng.toDouble, lat.toDouble))
      case _ => None
    }

  def fromRow(row: String): Option[Violation] = {
    val fields = parser.parseLine(row)
    parseLocation(fields(violationColumn("coordinates")))
      .map { point =>
        Violation(
          issueDate = parseTime(fields(violationColumn("issue_date"))),
          location = point,
          Ticket(
            description = fields(violationColumn("description")),
            fine = fields(violationColumn("fine")).replace("$","").toDouble
          )
        )
      }
  }
}
