package com.packt.spark

import geotrellis.vector._
import geotrellis.vector.io.json._
import spray.json._

import scala.io._

object Neighborhoods {

  case class NeighborhoodData(name: String, listName: String)

  implicit object NeighborhoodDataFormat extends RootJsonFormat[NeighborhoodData] {
    def write(x: NeighborhoodData) = ???

    def read(value: JsValue): NeighborhoodData = 
      value.asJsObject.getFields("name", "listname") match {
        case Seq(JsString(name), JsString(listName)) =>
          NeighborhoodData(name, listName)
        case _ => throw new DeserializationException("Couldn't read neighborhood data")
      }
  }

  def fromJson(path: String): Seq[MultiPolygonFeature[String]] =
      Source.fromFile(path)
        .getLines
        .mkString
        .parseGeoJson[JsonFeatureCollection]
        .getAllMultiPolygonFeatures[NeighborhoodData]
        .map { feature => MultiPolygonFeature(feature.geom, feature.data.name) }
}
