package com.packt.spark

import org.apache.spark.serializer.{KryoRegistrator => SparkKryoRegistrator}
import com.esotericsoftware.kryo.Kryo
import geotrellis.vector._

class KryoRegistrator extends SparkKryoRegistrator {
  override def registerClasses(kryo: Kryo) {
//    kryo.setRegistrationRequired(true)
    kryo.register(classOf[Violation])
    kryo.register(classOf[Ticket])
    kryo.register(classOf[section2.Serialization.TimeDelta])

    kryo.register(classOf[org.joda.time.DateTime], new de.javakaffee.kryoserializers.jodatime.JodaDateTimeSerializer)

    kryo.register(classOf[NeighborhoodData])
    kryo.register(classOf[MultiPolygon])
    kryo.register(classOf[Point])
    kryo.register(classOf[com.vividsolutions.jts.geom.Point])
    kryo.register(classOf[com.vividsolutions.jts.geom.MultiPolygon])
    kryo.register(classOf[com.vividsolutions.jts.geom.GeometryFactory])
    kryo.register(classOf[com.vividsolutions.jts.geom.impl.CoordinateArraySequenceFactory])
    kryo.register(classOf[com.vividsolutions.jts.geom.PrecisionModel])
    kryo.register(classOf[com.vividsolutions.jts.geom.PrecisionModel.Type])
    kryo.register(classOf[com.vividsolutions.jts.geom.Polygon])
    kryo.register(classOf[Array[com.vividsolutions.jts.geom.Polygon]])
    kryo.register(classOf[com.vividsolutions.jts.geom.LinearRing])
    kryo.register(classOf[Array[com.vividsolutions.jts.geom.LinearRing]])
    kryo.register(classOf[com.vividsolutions.jts.geom.Envelope])
    kryo.register(classOf[com.vividsolutions.jts.geom.impl.CoordinateArraySequence])
    kryo.register(classOf[com.vividsolutions.jts.geom.Coordinate])
    kryo.register(classOf[Array[com.vividsolutions.jts.geom.Coordinate]])
    kryo.register(classOf[Feature[_, _]])
    kryo.register(classOf[Array[scala.Tuple2[_, _]]])
  }
}
