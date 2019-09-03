import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph

object Tinkergraph extends App {
  Benchmarks.Tinkerpop3.benchmark(TinkerGraph.open)
}
