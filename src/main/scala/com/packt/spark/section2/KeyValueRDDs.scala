    // Learning Key Value Pair RDD
    // val neighborhoodCounts =
    //   neighborhoodViolations
    //     .groupBy { case (_, _, neighborhood) => neighborhood }
    //     .combineByKey(
    //       { violations: Iterable[(DateTime, Point, String)] => violations.size },
    //       { (count: Int, violations: Iterable[(DateTime, Point, String)]) => count + violations.size },
    //       { (count1: Int, count2: Int) => count1 + count2 }
    //      )
    //     .collect

    // for( (neighborhood, count) <- neighborhoodCounts) {
    //   println(s"$neighborhood   $count")
    // }
