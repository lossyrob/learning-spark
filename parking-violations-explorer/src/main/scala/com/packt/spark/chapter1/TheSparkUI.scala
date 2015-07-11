package com.packt.spark.chapter1

import org.apache.spark._

object SparkUI {
  def getSparkContext(): SparkContext = {
    val conf = 
      new SparkConf()
        .setMaster("local[4]")
        .setAppName("Discover PPA")

    new SparkContext(conf)
  }

  def main(args: Array[String]): Unit = {
    val sc = getSparkContext()

    // Wait for the user to give input, so that we have a chance to bring up the Spark UI.
    println("Press enter to continue..." )
    Console.readLine

    // Make sure to run the data/download-data.sh command.
    val path = "data/Parking_Violations.csv"

    val rdd =
      sc.textFile(path)

    val count = rdd.count

    println(s"Count is $count.")
  }
}
