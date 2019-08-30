import org.apache.tinkerpop.gremlin.structure.Graph
import org.apache.tinkerpop.gremlin.structure.io.IoCore
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphFactory
import scala.util.Using

object OrientDb extends App {
   val iterations = 10
   Using(newGraphWithData) { graph =>
     Benchmark.timed("g.V().outE().inV().outE().inV().outE().inV().toStream().count()", iterations) { () =>
       graph.traversal().V().outE().inV().outE().inV().outE().inV().toStream().count()
     }
   }

   def newGraphWithData(): Graph = {
      val graph = new OrientGraphFactory(s"memory:test-${Benchmark.newUUID}").getNoTx()
      graph.io(IoCore.graphml()).readGraph("../tinkerpop3/src/test/resources/grateful-dead.xml")
      graph
   }
}
