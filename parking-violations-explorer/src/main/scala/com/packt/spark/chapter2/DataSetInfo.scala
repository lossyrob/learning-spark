package com.packt.spark.chapter2

import com.packt.spark._
import org.apache.spark._

case class DataSetInfo(
  dateRange: DataDateRange = DataDateRange.empty, 
  validCount: Int = 0, 
  invalidCount: Int = 0,
  bigTicketItems: Set[(String, Double)] = Set(),
  totalFines: Double = 0.0
)

object DataSetInfo {
  implicit object DataSetInfoAccumulatorParam extends AccumulableParam[DataSetInfo, Option[Violation]] {
    def zero(info: DataSetInfo): DataSetInfo = DataSetInfo()

    def addInPlace(info1: DataSetInfo, info2: DataSetInfo): DataSetInfo = {
      val dateRange = DataDateRange.covering(info1.dateRange, info2.dateRange)
      val validCount = info1.validCount + info2.validCount
      val invalidCount = info1.invalidCount + info2.invalidCount
      val bigTicketItems = info1.bigTicketItems ++ info2.bigTicketItems
      val totalFines = info1.totalFines + info2.totalFines

      DataSetInfo(
        dateRange, 
        validCount, 
        invalidCount, 
        bigTicketItems,
        totalFines
      )
    }

    def addAccumulator(info: DataSetInfo, violationOption: Option[Violation]): DataSetInfo =
      violationOption match {
        case Some(violation) =>
          val dateRange = DataDateRange.covering(DataDateRange(violation.issueDate), info.dateRange)
          val validCount = info.validCount + 1
          val invalidCount = info.invalidCount
          val bigTicketItems = 
            if(violation.ticket.fine >= 1000.00)
              info.bigTicketItems + ((violation.ticket.description, violation.ticket.fine))
            else
              info.bigTicketItems

          val totalFines = info.totalFines + violation.ticket.fine

          DataSetInfo(
            dateRange,
            validCount,
            invalidCount,
            bigTicketItems,
            totalFines
          )
        case None =>
          DataSetInfo(
            info.dateRange,
            info.validCount,
            info.invalidCount + 1,
            info.bigTicketItems,
            info.totalFines
          )
      }
  }
}
