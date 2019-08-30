import org.apache.commons.configuration.BaseConfiguration
import org.apache.tinkerpop.gremlin.structure.Graph
import org.apache.tinkerpop.gremlin.structure.io.IoCore
import org.janusgraph.core.JanusGraphFactory
import scala.util.Using

object Janusgraph extends App {
   val iterations = 10
   Using(newGraphWithData) { graph =>
     Benchmark.timed("g.V().outE().inV().outE().inV().outE().inV().toStream().count()", iterations) { () =>
       graph.traversal().V().outE().inV().outE().inV().outE().inV().toStream().count()
     }
   }

   def newGraphWithData(): Graph = {
      val conf = new BaseConfiguration
      conf.setProperty("storage.backend","inmemory")
      val graph = JanusGraphFactory.open(conf)

      graph.io(IoCore.graphml()).readGraph("../tinkerpop3/src/test/resources/grateful-dead.xml")
      graph
   }
}
