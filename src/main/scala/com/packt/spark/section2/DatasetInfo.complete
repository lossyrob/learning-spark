package com.packt.spark.section2

import com.packt.spark._
import org.apache.spark._

case class DatasetInfo(
  dateRange: DataDateRange = DataDateRange.empty,
  validCount: Int = 0,
  bigTicketItems: Set[(String, Double)] = Set(),
  totalFines: Double = 0.0
)

object DatasetInfo {
  implicit object DatasetInfoAccumulatorParam extends AccumulableParam[DatasetInfo, Violation] {
    def zero(info: DatasetInfo): DatasetInfo = DatasetInfo()

    def addInPlace(info1: DatasetInfo, info2: DatasetInfo): DatasetInfo = {
      val dateRange = DataDateRange.covering(info1.dateRange, info2.dateRange)
      val validCount = info1.validCount + info2.validCount
      val bigTicketItems = info1.bigTicketItems ++ info2.bigTicketItems
      val totalFines = info1.totalFines + info2.totalFines

      DatasetInfo(
        dateRange, 
        validCount, 
        bigTicketItems,
        totalFines
      )
    }

    def addAccumulator(info: DatasetInfo, violation: Violation): DatasetInfo = {
      val dateRange = DataDateRange.covering(DataDateRange(violation.issueTime), info.dateRange)
      val validCount = info.validCount + 1
      val bigTicketItems =
        if(violation.ticket.fine >= 1000.00)
          info.bigTicketItems + ((violation.ticket.description, violation.ticket.fine))
        else
          info.bigTicketItems

      val totalFines = info.totalFines + violation.ticket.fine

      DatasetInfo(
        dateRange,
        validCount,
        bigTicketItems,
        totalFines
      )
    }
  }
}
