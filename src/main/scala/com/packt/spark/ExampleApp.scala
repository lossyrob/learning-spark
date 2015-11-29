package com.packt.spark

import org.apache.spark._
import org.apache.spark.rdd._

trait ExampleApp {
  def withSparkContext(f: SparkContext => Unit): Unit = {
    val conf =
      new SparkConf()
        .setMaster("local[4]")
        .setAppName("Learning Spark")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "com.packt.spark.KryoRegistrator")
//        .set("spark.default.parallelism", "10")
        .set("spark.cassandra.connection.host", "127.0.0.1")
        // .set("spark.cassandra.auth.username", "cassandra")            
        // .set("spark.cassandra.auth.password", "cassandra")

    val sc = new SparkContext(conf)

    try {
      f(sc)
    } finally {
      sc.stop()
    }
  }

  def csvDataset(path: String)(implicit sc: SparkContext): RDD[String] = {
    val absPath = new java.io.File(path).getAbsolutePath
    sc.textFile(s"file://$absPath")
  }

  def sampleDataset(implicit sc: SparkContext) =
    csvDataset("./data/Parking_Violations-sample.csv")

  def fullDataset(implicit sc: SparkContext) =
    csvDataset("./data/Parking_Violations.csv")

  def violations(implicit sc: SparkContext) =
    fullDataset
      .mapPartitions { rows =>
        val parse = Violation.rowParser
        rows.flatMap { row => parse(row) }
      }
      .filter(_.ticket.fine > 5.0)

  def sampleViolations(implicit sc: SparkContext) =
    sampleDataset
      .mapPartitions { rows =>
        val parse = Violation.rowParser
        rows.flatMap { row => parse(row) }
      }
      .filter(_.ticket.fine > 5.0)

  def waitForUser(): Unit = {
    println("Press enter key to continue...")
    Console.readLine
  }

  def main(args: Array[String]): Unit = 
    run()

  def run(): Unit
}
