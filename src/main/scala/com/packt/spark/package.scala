package com.packt

import geotrellis.vector._

package object spark {
  type Neighborhoods = Seq[Feature[Polygon, NeighborhoodData]]
}
