import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph

object Tinkergraph extends App {
  Benchmark.benchmark(TinkerGraph.open)
}
