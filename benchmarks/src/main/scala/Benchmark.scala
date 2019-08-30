import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.{GraphTraversal, GraphTraversalSource}
import org.apache.tinkerpop.gremlin.structure.io.IoCore
import org.apache.tinkerpop.gremlin.structure.Graph

object Benchmark {
  case class TestSetup(
    description: String,
    traversal: GraphTraversalSource => GraphTraversal[_, _],
    expectedResults: Long,
    iterations: Int)

  val testSetups = List(
    TestSetup(
      "warmup: g.V.outE.inV",
      _.V().outE().inV(),
      expectedResults = 8049,
      iterations = 1),
    TestSetup(
      "g.V.outE.inV.outE.inV.outE.inV",
      _.V().outE().inV().outE().inV().outE().inV(),
      expectedResults = 14465066,
      iterations = 100
    )
  )

  def benchmark(graph: Graph): Unit = {
    assert(graph.traversal.V().count.next() == 0, "graph must be empty")
    graph.io(IoCore.graphml()).readGraph("../tinkerpop3/src/test/resources/grateful-dead.xml")

    testSetups.foreach { test =>
      val millis = timed(test.iterations) { () =>
        val results = test.traversal(graph.traversal).toStream().count()
        assert(results == test.expectedResults, s"expected ${test.expectedResults} results, but got $results")
      }
      println(s"${test.description}: $millis")
    }
    graph.close
  }

  /* returns the average time in millis */
  def timed(iterations: Int)(fun: () => Unit): Float = {
    val start = System.nanoTime
    1.to(iterations).foreach { _ => fun()}
    val average = (System.nanoTime - start) / iterations.toFloat / 1_000_000f
    average
  }

  def newUUID(): String =
    java.util.UUID.randomUUID.toString.substring(0, 16)

}
