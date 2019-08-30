import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraph
import org.apache.tinkerpop.gremlin.structure.Graph
import org.apache.tinkerpop.gremlin.structure.io.IoCore
import scala.util.Using

object Neo4j extends App {
   val iterations = 10
   Using(newGraphWithData) { graph =>
     Benchmark.timed("g.V().outE().inV().outE().inV().outE().inV().toStream().count()", iterations) { () =>
       graph.traversal().V().outE().inV().outE().inV().outE().inV().toStream().count()
     }
   }

   def newGraphWithData(): Graph = {
      val graph = Neo4jGraph.open(s"neo4j/target/testdb-${Benchmark.newUUID}")
      graph.io(IoCore.graphml()).readGraph("../tinkerpop3/src/test/resources/grateful-dead.xml")
      graph
   }
}
