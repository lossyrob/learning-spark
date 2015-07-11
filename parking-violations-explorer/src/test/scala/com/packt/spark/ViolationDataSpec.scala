package com.packt.spark

import geotrellis.vector._
import org.scalatest._

class ViolationDataSpec extends FunSpec 
    with Matchers {
  describe("ViolationData.parseLocation") {
    it("should parse a valid location") {
      val s = "(39.9598882869,-75.1484695504)"
      val parsed = ViolationData.parseLocation(s)
      parsed.isDefined should be (true)
      parsed.get should be (Point(-75.1484695504, 39.9598882869))
    }

    it("should return None for empty string") {
      val s = ""
      val parsed = ViolationData.parseLocation(s)
      parsed.isDefined should be (false)
    }
  }
}
