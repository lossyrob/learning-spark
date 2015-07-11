package com.packt.spark.chapter1

import com.packt.spark._

import org.apache.spark._

object TransformationsAndActions {
  val dataPath = "data/Parking_Violations-sample.csv"

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

    val violationEntries =
      sc.textFile(dataPath)
        .filter(!_.startsWith("Issue"))
        .flatMap(Violation.fromRow _)

    val count = violationEntries.count

    println(s"Count is $count.")

    // Get distinct violations.
    val violationTypes =
      violationEntries.map(_.ticket.description).distinct

    println("Violation Types:")
    violationTypes.foreach { desc => println(s" $desc") }

    // Find max fine.
    val maxFineValue = violationEntries.map(_.ticket.fine).max
    val minFineValue = violationEntries.map(_.ticket.fine).max

    println(s"Min fine value: $minFineValue")
    println(s"Max fine value: $maxFineValue")

    val maxFine = 
      violationEntries
        .map(_.ticket.fine)
        .reduce { (fine1, fine2) =>
          if(fine1 > fine2) fine1
          else fine2
        }

    println(s"Max fine: $maxFine")

    val bigTicketItems = 
      violationEntries
        .filter(_.ticket.fine > 1000.00)
        .map(_.ticket.description)
        .distinct
        .collect
  }
}
