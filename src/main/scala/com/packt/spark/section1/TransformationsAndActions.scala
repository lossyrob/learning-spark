package com.packt.spark.section1

import com.packt.spark._
import org.apache.spark._

object TransformationsAndActions extends ExampleApp {
  def run() =
    withSparkContext { implicit sc =>
      val maxFine =
        violations.map(_.ticket.fine).max

      val minFine =
        violations.map(_.ticket.fine).min

      println(s"Maximum fine amount: $maxFine")
      println(s"Minimum fine amount: $minFine")

      val maxTicket: Ticket =
        violations
          .map(_.ticket)
          .reduce { (ticket1, ticket2) =>
            if(ticket1.fine > ticket2.fine) ticket1
            else ticket2
           }

      val minTicket: Ticket =
        violations
          .map(_.ticket)
          .reduce { (ticket1, ticket2) =>
            if(ticket1.fine < ticket2.fine) ticket1
            else ticket2
           }

      println(s"Maximum Ticket: $maxTicket")
      println(s"Minium Ticket: $minTicket")

      val ticketCount =
        violations
          .map(_.ticket)
          .distinct
          .count

      val descriptions =
        violations
          .map(_.ticket.description)
          .distinct
          .collect

      val descriptionCount = descriptions.size

      println(s"Number of distinct tickets: $ticketCount")
      println(s"Number of distinct ticket descriptions: $descriptionCount")
      println("Violation Types:")
      descriptions.sorted.foreach { desc => println(s" $desc") }

      val (sum, count): (Double, Double) =
        violations
          .map(_.ticket.fine)
          .aggregate( (0.0, 0.0))(
            { (acc, fine) => (acc._1 + fine, acc._2 + 1) }, 
            { (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2) }
          )

      val mean = sum / count

      println(f"\nSum is $$${sum}%,1.2f")
      println(f"\nMean is $$${mean}%,1.2f\n")
    }
}
