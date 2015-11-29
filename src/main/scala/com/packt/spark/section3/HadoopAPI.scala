package com.packt.spark.section3

import com.packt.spark._
import org.apache.spark._
import org.apache.spark.rdd._
import geotrellis.vector._
import com.github.nscala_time.time.Imports._

import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, SequenceFileInputFormat}
import org.apache.hadoop.io.{Writable, WritableComparable}
import java.io._

class NeighborhoodDataWritable() extends Writable {
  private var _value: NeighborhoodData = null

  def set(value: NeighborhoodData): Unit = _value = value
  def get(): NeighborhoodData = _value

  def readFields(in: DataInput): Unit = {
    val name = in.readUTF()
    val area = in.readDouble()
    _value = NeighborhoodData(name, area)
  }

  def write(out: DataOutput): Unit = {
    out.writeUTF(_value.name)
    out.writeDouble(_value.area)
  }
}

object NeighborhoodDataWritable {
  def apply(neighborhoodData: NeighborhoodData): NeighborhoodDataWritable = {
    val writable = new NeighborhoodDataWritable()
    writable.set(neighborhoodData)
    writable
  }
}

class ViolationWritable() extends Writable {
  private var _value: Violation = null

  def set(value: Violation): Unit = _value = value
  def get(): Violation = _value


  def readFields(in: DataInput): Unit = {
    val issueTime = DateTime.parse(in.readUTF(), ViolationWritable.dateTimeFormat)
    val x = in.readDouble()
    val y = in.readDouble()
    val state = in.readUTF()
    val agency = in.readUTF()
    val fine = in.readDouble()
    val description = in.readUTF()
    _value = Violation(issueTime, Point(x, y), state, agency, Ticket(fine, description))
  }

  def write(out: DataOutput): Unit = {
    out.writeUTF(ViolationWritable.dateTimeFormat.print(_value.issueTime))
    out.writeDouble(_value.location.x)
    out.writeDouble(_value.location.y)
    out.writeUTF(_value.state)
    out.writeUTF(_value.agency)
    out.writeDouble(_value.ticket.fine)
    out.writeUTF(_value.ticket.description)
  }
}

object ViolationWritable {
  def dateTimeFormat = DateTimeFormat.forPattern("MM/DD/YYYY HH:mm:ss aa")

  def apply(violation: Violation): ViolationWritable = {
    val writable = new ViolationWritable
    writable.set(violation)
    writable
  }
}

object HadoopAPI extends ExampleApp {
  def run() =
    withSparkContext { implicit sc =>
      val path = "s3n://packt-spark-example/test/TEST-OUT-SPARK-S3"

      write(path)
//      read(path)
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

    neighborhoodsToViolations
      .sample(false, 0.05)
      .map { case (key, value) => (NeighborhoodDataWritable(key), ViolationWritable(value)) }
      .saveAsSequenceFile(path)
  }

  def read(path: String)(implicit sc: SparkContext): Unit = {
    val readRDD: RDD[(NeighborhoodData, Violation)] =
      sc.sequenceFile(
        path,
        classOf[NeighborhoodDataWritable],
        classOf[ViolationWritable]
      ).map { case (keyW, valueW) => (keyW.get, valueW.get) }

    println(readRDD.count)
  }
}
