import org.apache.tinkerpop.gremlin.orientdb.OrientGraphFactory

object OrientDb extends App {
  val factory = new OrientGraphFactory(s"memory:test-${Benchmark.newUUID}")
  Benchmark.benchmark(factory.getNoTx)
  factory.close()
}
