package com.packt.spark

import geotrellis.vector._
import org.scalatest._
import com.github.nscala_time.time.Imports._

class ViolationSpec extends FunSpec 
    with Matchers {
  describe("Violation.parseTime") {
    it("should parse a valid date form our dataset") {
      val s = "01/01/2012 12:00:00 AM"
      val parsed = Violation.parseTime(s)
      parsed should be (new DateTime(2012, 1, 1, 12, 0, 0))
    }
  }

  describe("Violation.parseLocation") {
    it("should parse a valid location") {
      val s = "(39.9598882869,-75.1484695504)"
      val parsed = Violation.parseLocation(s)
      parsed.isDefined should be (true)
      parsed.get should be (Point(-75.1484695504, 39.9598882869))
    }

    it("should return None for empty string") {
      val s = ""
      val parsed = Violation.parseLocation(s)
      parsed.isDefined should be (false)
    }
  }
}
