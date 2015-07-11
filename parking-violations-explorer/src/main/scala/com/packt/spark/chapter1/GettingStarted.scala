package com.packt.spark.chapter1

import org.apache.spark._

object GettingStarted {
  def getSparkContext(): SparkContext = {
    val conf = 
      new SparkConf()
        .setMaster("local[4]") // Don't set if using spark-submit
        .setAppName("Discover PPA")

    new SparkContext(conf)
  }

  def main(args: Array[String]): Unit = {
    val sc = getSparkContext()

    val path = "data/Parking_Violations-sample.csv"

    val rdd =
      sc.textFile(path)

    val count = rdd.count

    println(s"Count is $count.")
  }
}
