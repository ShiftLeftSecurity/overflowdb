import org.apache.tinkerpop.gremlin.structure.io.IoCore
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph

import scala.util.Using

object Tinkergraph extends App {
   val iterations = 10
//   val graph = newGraphWithData()
   Using(newGraphWithData) { graph =>
     Benchmark.timed("g.V().outE().inV().outE().inV().outE().inV().toStream().count()", iterations) { () =>
       graph.traversal().V().outE().inV().outE().inV().outE().inV().toStream().count()
     }
   }

   def newGraphWithData(): TinkerGraph = {
      val graph = TinkerGraph.open
      graph.io(IoCore.graphml()).readGraph("../tinkerpop3/src/test/resources/grateful-dead.xml")
      graph
   }
}
