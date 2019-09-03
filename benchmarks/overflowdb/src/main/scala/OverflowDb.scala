import Benchmarks.{loadGratefulDead, timed}
import io.shiftleft.overflowdb.testdomains.gratefuldead.GratefulDead
import org.apache.tinkerpop.gremlin.structure.{Direction, Graph}

import scala.jdk.CollectionConverters._

object OverflowDbTinkerpop3 extends App {
  Benchmarks.Tinkerpop3.benchmark(GratefulDead.newGraph)
}

/**
 * compare performance of overflowdb tinkerpop api with standard collections.
 *
 * timings on my machine:
 * java forEachRemaining: 7.093274
 * scala foreach: 4.868037
 * scala map - flatten at the end: 14.794024
 * scala map.flatten every step: 36.321968
 * scala flatMap: 32.51532
 *
 * interpretation:
 * 1) scala's foreach is faster than java's
 * 2) flatMap is always more expensive, but standard collections are roughly twice as fast as tinkerpop
 */
object OdbTp3VsCollectionsPerformance extends App {
  benchmark(GratefulDead.newGraph)

  def benchmark(graph: Graph): Unit = {
    loadGratefulDead(graph)

    testSetups.foreach { test =>
      val millis = timed(test.iterations) { () =>
        val results = test.traversal(graph)
        assert(results == test.expectedResults, s"expected ${test.expectedResults} results, but got $results")
      }
      println(s"${test.description}: $millis")
    }
    graph.close
  }

  case class TestSetup(description: String,
                       traversal: Graph => Int,
                       expectedResults: Long,
                       iterations: Int)

  lazy val testSetups = List(
    TestSetup(
      "warmup",
      _.vertices().asScala.flatMap { vertex =>
        vertex.vertices(Direction.OUT).asScala
      }.size,
      expectedResults = 8049,
      iterations = 100),
    TestSetup("java forEachRemaining",
      graph => {
        var results = 0
        graph.vertices().forEachRemaining { vertex =>
          vertex.vertices(Direction.OUT).forEachRemaining { vertex =>
            vertex.vertices(Direction.OUT).forEachRemaining { vertex =>
              results += 1
            }
          }
        }
        results
      },
      327370,
      100),
    TestSetup("scala foreach",
      graph => {
        var results = 0
        graph.vertices().asScala.foreach { vertex =>
          vertex.vertices(Direction.OUT).asScala.foreach { vertex =>
            vertex.vertices(Direction.OUT).asScala.foreach { vertex =>
              results += 1
            }
          }
        }
        results
      },
      327370,
      100),
    TestSetup("scala map - flatten at the end",
      _.vertices().asScala.map { vertex =>
        vertex.vertices(Direction.OUT).asScala.map { vertex =>
          vertex.vertices(Direction.OUT).asScala.map { vertex =>
            1
          }
        }
      }.flatten.flatten.sum,
      327370,
      100),
    TestSetup("scala map.flatten every step",
      _.vertices().asScala.map { vertex =>
        vertex.vertices(Direction.OUT).asScala.map { vertex =>
          vertex.vertices(Direction.OUT).asScala.toSeq.map { vertex =>
            1
          }
        }.flatten
      }.flatten.sum,
      327370,
      100),
    TestSetup("scala flatMap",
      _.vertices().asScala.flatMap { vertex =>
        vertex.vertices(Direction.OUT).asScala.flatMap { vertex =>
          vertex.vertices(Direction.OUT).asScala
        }
      }.size,
      327370,
      100),
  )
}