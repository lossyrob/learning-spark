package com.packt.spark

import geotrellis.vector._
import geotrellis.vector.io.json._
import spray.json._

import scala.io._

case class NeighborhoodData(name: String, area: Double)

object Neighborhoods {
  implicit object NeighborhoodDataPropertyReader extends JsonReader[(String, Double)] {
    def read(value: JsValue): (String, Double)= 
      value.asJsObject.getFields("name", "shape_area") match {
        case Seq(JsString(name), JsNumber(area)) =>
          (name, area.toDouble)
        case _ => throw new DeserializationException("Couldn't read neighborhood data")
      }
  }

  def fromJson(path: String): Seq[MultiPolygonFeature[NeighborhoodData]] = {
    val neighborhoods = 
      GeoJson.fromFile[JsonFeatureCollection](path)
        .getAllMultiPolygonFeatures[(String, Double)]

    val totalArea = neighborhoods.map(_.data._2).sum

    neighborhoods.map { feature =>
      feature.mapData { 
        case (name, area) => NeighborhoodData(name, (area * 100) / totalArea)
      }
    }

  }
}
