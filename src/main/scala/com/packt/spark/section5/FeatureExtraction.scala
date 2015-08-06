package com.packt.spark.section5

import com.packt.spark._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.mllib._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.evaluation._
import geotrellis.vector._

import org.apache.spark.storage._
import com.github.nscala_time.time.Imports._
import org.joda.time._

// case class ViolationFeature(
//   neighborhood: Int,
//   hour: Int,
//   densityAllTime: Double,
//   densityForMonthOfYear: Double,
//   densityForDayOfYear: Double,
//   densityForDayOfMonth: Double,
//   densityForDayOfWeek: Double,
//   densityForHourOfDay: Double
// ) {
//   def toVector =
//     Vector(neighborhood.toDouble, hour.toDouble, densityAllTime, densityForMonthOfYear, densityForDayOfMonth, densityForDayOfWeek, densityForHourOfDay)
// }

// object ViolationFeature {
//   val baseDateTime = new DateTime(2010, 1, 1, 0, 0)
//   def getHour(violation: Violation): Int =
//     Hours.hoursBetween(baseDateTime, violation.issueTime).getHours

//   def extractDateInfo(hour: Int) = {
//     val dt = baseDateTime.plusHours(hour)
//     (dt.getMonthOfYear,
//       dt.getDayOfYear,
//       dt.getDayOfMonth,
//       dt.getDayOfWeek,
//       dt.getHourOfDay)
//   }

// }

// object FeatureExtraction extends ExampleApp {
//   def run() =
//     withSparkContext { implicit sc =>
//       val neighborhoods = Neighborhoods()
//       val bcNeighborhoods = sc.broadcast(neighborhoods)
//       val bcDensityAggregations = sc.broadcast(section2.Caching.densityAggregations(neighborhoods))

//       val points: RDD[LabeledPoint] =
//       violations
//         .flatMap { violation =>
//           bcNeighborhoods.value
//             .find(_.geom.contains(violation.location))
//             .map { case Feature(_, data) =>
//               (violation, data.name)
//           }
//         }
//         .map { case (violation, neighborhood) =>
//           ((neighborhood, ViolationFeature.getHour(violation)), 1)
//          }
//         .reduceByKey(_ + _)
//         .map { case ((neighborhood, hour), count) =>
//           val densityAggregations = bcDensityAggregations.value
//           val (monthOfYear, dayOfYear, dayOfMonth, dayOfWeek, hourOfDay) =
//             ViolationFeature.extractDateInfo(hour)

//           val neighborhoodIds = bcNeighborhoods.value.map(_.data.name).zipWithIndex.toMap
//           LabeledPoint(
//             count.toDouble,
//             Vectors.dense(
//               Array(
//                 neighborhoodIds(neighborhood).toDouble,
//                 hour.toDouble,
//                 densityAggregations("allTime")((neighborhood, 0)),
//                 densityAggregations("monthOfYear")((neighborhood, monthOfYear)),
//                 densityAggregations("dayOfYear")((neighborhood, dayOfYear)),
//                 densityAggregations("dayOfMonth")((neighborhood, dayOfMonth)),
//                 densityAggregations("dayOfWeek")((neighborhood, dayOfWeek)),
//                 densityAggregations("hourOfDay")((neighborhood, hourOfDay))
//               )
//             )
//           )
//         }

//       val splits = points.randomSplit(Array(0.7, 0.3))
//       val (trainingData, testData) = (splits(0), splits(1))

//       val categoricalFeaturesInfo = Map[Int, Int]( (0, neighborhoods.size) )
//       val numTrees = 100
//       val featureSubsetStrategy = "auto" // Let the algorithm choose.
//       val impurity = "variance"
//       val maxDepth = 8
//       val maxBins = 500

//       val model = RandomForest.trainRegressor(trainingData, categoricalFeaturesInfo,
//         numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

//       val labelsAndPredictions = testData.map { point =>
//         val prediction = model.predict(point.features)
//         (point.label, prediction)
//       }

//       val metrics = new RegressionMetrics(labelsAndPredictions)

// //      println("Test Mean Squared Error = " + testMSE)
//       println(f"rmse ${metrics.rootMeanSquaredError}%.3f")
//       println(f"r2 ${metrics.r2}%.3f")
//       println(f"mae ${metrics.meanAbsoluteError}%.3f")
// //      println("Learned regression forest model:\n" + model.toDebugString)

//       waitForUser()
//     }
// }
