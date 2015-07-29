// package com.packt.spark.section2

// import com.packt.spark._
// import org.apache.spark._
// import geotrellis.vector._

// import org.apache.spark.storage._
// import com.github.nscala_time.time.Imports._

// ViolationFeatures(
//   issueTime: DateTime,
//   location: Point,
//   neighborhood: String,
//   densityAllTime: Double,
//   densityForDayOfYear: Double,
//   densityForDayOfMonth: Double,
//   densityForMontOfYear


// object Cassandra extends ExampleApp {
//   def run() =
//     withSparkContext { implicit sc =>
//       val neighborhoods = Neighborhoods()
//       val bcDensityAggregations = sc.broadcast(section2.Caching.densityAggregations(neighborhoods))

//       violations
//         .map { 

//       waitForUser()
//     }
// }
