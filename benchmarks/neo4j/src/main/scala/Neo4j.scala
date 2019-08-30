import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraph

object Neo4j extends App {
  Benchmark.benchmark(Neo4jGraph.open(s"neo4j/target/testdb-${Benchmark.newUUID}"))
}
