package com.packt.spark

import org.apache.spark._

package object util {
  def getSparkContext(): SparkContext = {
    val conf =
      new SparkConf()
        .setMaster("local[4]")
        .setAppName("Coding with Spark")

    new SparkContext(conf)
  }

  def withSparkContext(f: SparkContext => Unit): Unit = {
    val sc = getSparkContext
    try {
      f(sc)
    } finally {
      sc.stop()
    }
  }

  def waitForUserInput(f: => Unit): Unit = {
    println("Press enter to continue..." )
    Console.readLine

    f

    println("Press enter to continue..." )
    Console.readLine
  }
}
