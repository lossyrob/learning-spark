package com.packt.spark.section1

import com.packt.spark.util._

import org.apache.spark._

object SparkUI {
  def main(args: Array[String]): Unit =
    withSparkContext { sc =>
      waitForUserInput { 
        // Make sure to run the data/download-data.sh command.
        val path = "../data/Parking_Violations.csv"

        val rdd =
          sc.textFile(path)

        val count = rdd.count

        println(s"\nCount is $count.\n")
      }
    }
}
