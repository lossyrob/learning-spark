package com.packt.spark.section2

import com.packt.spark._

import org.apache.spark._
import com.github.nscala_time.time.Imports._

object Accumulators extends ExampleApp {
  implicit object MaxFineAccumulatorParam extends AccumulatorParam[Ticket] {
    def zero(ticket: Ticket) = Ticket(0.0, "None")
    def addInPlace(ticket1: Ticket, ticket2: Ticket): Ticket =
      if(ticket1.fine > ticket2.fine) ticket1
      else ticket2
  }
  def run() =
    withSparkContext { implicit sc =>
      val validAcc = sc.accumulator(0)
      val invalidAcc = sc.accumulator(0)
      val sumAcc = sc.accumulator(0.0)
      val maxFineAcc = sc.accumulator(Ticket(0.0, "None"))
      val dateRangeAcc = sc.accumulator(DataDateRange.empty)

      val violationEntries = 
        sampleDataset
          .flatMap { line =>
            Violation.fromRow(line) match {
              case e @ Some(entry) =>
                validAcc += 1
                sumAcc += entry.ticket.fine
                maxFineAcc += entry.ticket
                dateRangeAcc += entry.issueTime
                e
              case None =>
                invalidAcc += 1
                None
            }
          }

      violationEntries.foreach { x =>  }

      val validCount = validAcc.value
      val invalidCount = invalidAcc.value
      val totalFines = sumAcc.value
      val maxFineTicket = maxFineAcc.value

      println(s"Valid count: ${validAcc.value}")
      println(s"Invalid count: ${invalidAcc.value}")
      println(f"Total fines: $$${sumAcc.value.toLong}%,d")
      println(s"Max fine ticket: $maxFineTicket")
      println(s"Date range: ${dateRangeAcc.value.start} to ${dateRangeAcc.value.end}")
    }
}
