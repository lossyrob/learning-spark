package com.packt.spark.section1

import com.packt.spark._

import org.apache.spark._

object TransformationsAndActions extends ExampleApp {
  def run() =
    withSparkContext { implicit sc =>
      val (sum, count): (Double, Double) =
        fullDataset
          .flatMap(Violation.fromRow _)
          .map(_.ticket.fine)
          .aggregate( (0.0, 0.0))( 
            { (acc, fine) => (acc._1 + fine, acc._2 + 1) }, 
            { (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2) }
          )

      val mean = sum / count

      println(f"\nSum is $$${sum}%,1.2f\n")
      println(f"\nMean is $$${mean}%,1.2f\n")

      waitForUser()
    }
}
