import org.apache.tinkerpop.gremlin.orientdb.OrientGraphFactory

object OrientDb extends App {
  val factory = new OrientGraphFactory(s"memory:test-${Benchmarks.newUUID}")
  Benchmarks.Tinkerpop3.benchmark(factory.getNoTx)
  factory.close()
}
