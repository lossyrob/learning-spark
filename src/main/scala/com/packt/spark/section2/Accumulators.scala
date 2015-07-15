package com.packt.spark.section2

import com.packt.spark._

import org.apache.spark._
import org.apache.spark.SparkContext._
import com.github.nscala_time.time.Imports._

// object Accumulators {
//   val dataPath = "data/Parking_Violations.csv"

//   def getSparkContext(): SparkContext = {
//     val conf = 
//       new SparkConf()
//         .setMaster("local[4]")
//         .setAppName("Accumulators")

//     new SparkContext(conf)
//   }

//   def main(args: Array[String]): Unit = {
//     val sc = getSparkContext()

//     val infoAcc = sc.accumulable(DataSetInfo())

//     val violationEntries =
//       sc.textFile(dataPath)
//         .filter(!_.startsWith("Issue"))
//         .flatMap { line =>
//           val parsed = Violation.fromRow(line)
//           infoAcc += parsed
//           parsed
//         }

//     // More computations would happen here...
//     violationEntries
//       .filter(_.ticket.fine == 2000.0)
//       .collect
//       .foreach(println(_))

//     // val DataSetInfo(
//     //   dateRange,
//     //   validCount,
//     //   invalidCount,
//     //   bigTicketItems,
//     //   totalFines
//     // ) = infoAcc.value

//     // println(s"Valid count: $validCount")
//     // println(s"Invalid count: $invalidCount")
//     // println(s"Date range: ${dateRange.start} to ${dateRange.end}")
//     // println("Big ticket items:")
//     // for( (desc, fine) <- bigTicketItems) {
//     //   println(s" $fine  $desc")
//     // }
//     sc.stop
//   }
// }
