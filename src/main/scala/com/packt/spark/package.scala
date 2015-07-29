package com.packt

import geotrellis.vector._

package object spark {
  type Neighborhoods = Seq[MultiPolygonFeature[NeighborhoodData]]
}
