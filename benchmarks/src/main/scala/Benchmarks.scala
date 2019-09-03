import java.util.{Iterator => JIterator}

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.__
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.{GraphTraversal, GraphTraversalSource}
import org.apache.tinkerpop.gremlin.structure.io.IoCore
import org.apache.tinkerpop.gremlin.structure.{Edge, Graph, Vertex}

object Benchmarks {

  object Tinkerpop3 {
    def benchmark(graph: Graph): Unit = {
      loadGratefulDead(graph)

      smallGraphTestSetups.foreach { test =>
        val millis = timed(test.iterations) { () =>
          val results = test.traversal(graph.traversal).toStream().count()
          assert(results == test.expectedResults, s"expected ${test.expectedResults} results, but got $results")
        }
        println(s"${test.description}: $millis")
      }
    }

    case class TestSetup(description: String,
                         traversal: GraphTraversalSource => GraphTraversal[_, _],
                         expectedResults: Long,
                         iterations: Int)

    /** from https://robertdale.github.io/2017/01/26/gremlin-neo4j-driver-benchmarks.html */
    val smallGraphTestSetups = List(
      TestSetup(
        "warmup",
        _.V().outE().inV,
        expectedResults = 8049,
        iterations = 100),
      TestSetup(
        "g.V.outE.inV.outE.inV.outE.inV",
        _.V().outE().inV.outE().inV.outE().inV,
        expectedResults = 14465066,
        iterations = 100),
      TestSetup(
        "g.V.out.out.out",
        _.V().out().out().out(),
        expectedResults = 14465066,
        iterations = 100),
      TestSetup(
        "g.V.out.out.out.path",
        _.V().out().out().out().path,
        expectedResults = 14465066,
        iterations = 10),
      TestSetup(
        "g.V.repeat(out()).times(2)",
        _.V().repeat(__().out()).times(2),
        expectedResults = 327370,
        iterations = 100),
      TestSetup(
        "g.V.repeat(out()).times(3)",
        _.V().repeat(__().out()).times(3),
        expectedResults = 14465066,
        iterations = 100),
      TestSetup(
        "g.V.local(out().out().values(\"name\").fold)",
        _.V().local(__().out().out().values("name").fold()),
        expectedResults = 808,
        iterations = 100),
      TestSetup(
        "g.V.out.local(out.out.values(\"name\").fold)",
        _.V().out().local(__().out().out().values("name").fold()),
        expectedResults = 562,
        iterations = 100),
      TestSetup(
        "g.V.outE",
        _.V().outE(),
        expectedResults = 8049,
        iterations = 100),
      TestSetup(
        "g.V.outE via flatMap",
        _.V().flatMap { trav =>
          __(trav.get).outE(): JIterator[Edge]
        },
        expectedResults = 8049,
        iterations = 100),
      TestSetup(
        "g.V.outE.inV",
        _.V().outE().inV,
        expectedResults = 8049,
        iterations = 100),
      TestSetup(
        "g.V.outE.inV via flatMap",
        _.V().flatMap { trav =>
          __(trav.get).outE().flatMap { trav =>
            __(trav.get).inV
          }: JIterator[Vertex]
        },
        expectedResults = 8049,
        iterations = 100),
      TestSetup(
        "g.V.out.out",
        _.V().out().out(),
        expectedResults = 327370,
        iterations = 100),
      TestSetup(
        "g.V.out.out via flatMap",
        _.V().flatMap { trav =>
          __(trav.get).out().flatMap { trav =>
            __(trav.get).out()
          }: JIterator[Vertex]
        },
        expectedResults = 327370,
        iterations = 100),
    )
  }

  def loadGratefulDead(graph: Graph): Graph = {
    assert(graph.traversal.V().count.next() == 0, "graph must be empty")
    graph.io(IoCore.graphml()).readGraph("../tinkerpop3/src/test/resources/grateful-dead.xml")
    graph
  }

  /* returns the average time in millis */
  def timed(iterations: Int)(fun: () => Unit): Float = {
    val start = System.nanoTime
    1.to(iterations).foreach { _ => fun() }
    val average = (System.nanoTime - start) / iterations.toFloat / 1_000_000f
    average
  }

  def newUUID(): String =
    java.util.UUID.randomUUID.toString.substring(0, 16)

}
