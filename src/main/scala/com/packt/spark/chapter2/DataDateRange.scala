package com.packt.spark.chapter2

import org.apache.spark._
import com.github.nscala_time.time.Imports._

case class DataDateRange(start: DateTime, end: DateTime)

object DataDateRange {
  private val emptyDate = new DateTime(1000, 1, 1, 0, 0)

  def empty = DataDateRange(emptyDate)

  def apply(dateTime: DateTime): DataDateRange =
    DataDateRange(dateTime, dateTime)

  implicit def dateTimeToRange(dateTime: DateTime): DataDateRange =
    apply(dateTime)

  def covering(range1: DataDateRange, range2: DataDateRange): DataDateRange =
    if(range1.start == emptyDate) range2
    else {
      if(range2.start == emptyDate) range1
      else {
        val start =
          if(range1.start < range2.start) range1.start
          else range2.start

        val end =
          if(range1.end > range2.end) range1.end
          else range2.end

        DataDateRange(start, end)
      }
    }

  implicit object DataDateRangeAccumulatorParam extends AccumulatorParam[DataDateRange] {
    def zero(range: DataDateRange): DataDateRange = empty
    def addInPlace(range1: DataDateRange, range2: DataDateRange): DataDateRange =
      DataDateRange.covering(range1, range2)
  }
}
