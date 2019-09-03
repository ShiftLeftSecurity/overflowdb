import org.apache.commons.configuration.BaseConfiguration
import org.janusgraph.core.JanusGraphFactory

object Janusgraph extends App {
  val conf = new BaseConfiguration
  conf.setProperty("storage.backend", "inmemory")
  val graph = JanusGraphFactory.open(conf)
  Benchmarks.Tinkerpop3.benchmark(graph)
}
