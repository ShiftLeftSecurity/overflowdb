import io.shiftleft.overflowdb.testdomains.gratefuldead.GratefulDead

import scala.util.Using

object OverflowDb extends App {
  val iterations = 10
  Using(GratefulDead.newGraphWithData) { graph =>
    Benchmark.timed("g.V().outE().inV().outE().inV().outE().inV().toStream().count()", iterations) { () =>
      graph.traversal().V().outE().inV().outE().inV().outE().inV().toStream().count()
    }
  }
}
