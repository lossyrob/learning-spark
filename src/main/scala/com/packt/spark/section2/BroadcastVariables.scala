//  case class NeighborhoodData(name: String, listName: String)

    // val neighborhoods = Neighborhoods.fromJson("data/Neighborhoods_Philadelphia.geojson")
    // val bcNeighborhoods = sc.broadcast(neighborhoods)

    // val neighborhoodViolations = 
    //   violationEntries.map { violationEntry =>
    //     bcNeighborhoods.value.find(_.geom.contains(violationEntry.location)) match {
    //       case Some(mpf) =>
    //         (violationEntry.issueDate, violationEntry.location, mpf.data)
    //       case None =>
    //         (violationEntry.issueDate, violationEntry.location, "UNKNOWN")
    //       }
    //     }

