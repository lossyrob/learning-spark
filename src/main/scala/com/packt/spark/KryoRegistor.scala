package com.packt.spark

import org.apache.spark.serializer.{KryoRegistrator => SparkKryoRegistrator}
import com.esotericsoftware.kryo.Kryo

class KryoRegistrator extends SparkKryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[Violation])
    kryo.register(classOf[Ticket])

    kryo.register(classOf[org.joda.time.DateTime], new de.javakaffee.kryoserializers.jodatime.JodaDateTimeSerializer)
  }
}

