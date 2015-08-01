package com.packt.spark.section2

import com.packt.spark._

import org.apache.spark._
import com.github.nscala_time.time.Imports._

object AdvancedAccumulators extends ExampleApp {
  def run() =
    withSparkContext { implicit sc =>
      val invalidAcc = sc.accumulator(0)
      val infoAcc = sc.accumulable(DatasetInfo())

      val violationEntries = 
        fullDataset
          .mapPartitions { partition =>
            val parse = Violation.rowParser
            partition.flatMap { line =>
              parse(line) match {
                case v @ Some(violation) if violation.ticket.fine > 5.0 =>
                  infoAcc += violation
                  v
                case _ =>
                  invalidAcc += 1
                  None
              }
            }
          }

      violationEntries.foreach { x =>  }

      val DatasetInfo(
        dateRange,
        validCount,
        bigTicketItems,
        totalFines
      ) = infoAcc.value

      val invalidCount = invalidAcc.value

      println(s"Valid count is $validCount")
      println(s"Invalid count is $invalidCount")
      println(f"Total fines: $$${totalFines}%,1.2f")
      println(s"Date range: ${dateRange.start} to ${dateRange.end}")
      println("Big ticket items:")
      for( (desc, fine) <- bigTicketItems) {
        println(s" $fine  $desc")
      }
      waitForUser()
    }
}
