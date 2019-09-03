import Benchmarks.loadGratefulDead
import io.shiftleft.overflowdb.testdomains.gratefuldead.GratefulDead
import java.util.{Iterator => JIterator}
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.__
import org.apache.tinkerpop.gremlin.structure.{Direction, Vertex}
import scala.jdk.CollectionConverters._

/**
 * compare amount of garbage created for tinkerpop api and standard collections
 *
 * measurements on my machine:
 * time overflowdb/target/universal/stage/bin/memory-impact-scala-collections -J-Xms256m -J-Xmx256m -J-Xloggc:gc-scala-collections.log
 * 27s, 25 young gc runs
 * time overflowdb/target/universal/stage/bin/memory-impact-tp-3 -J-Xms256m -J-Xmx256m -J-Xloggc:gc-scala-collections.log
 * 47s, 654 young gc runs
 *
 * interpretation:
 * tinkerpop's flatMap/start operations are incredibly memory hungry and creates 25x more garbage than plain scala collections
 */
object Tp3VsCollectionsMemoryImpact

object MemoryImpactScalaCollections extends App {
  val graph = loadGratefulDead(GratefulDead.newGraph)

  1.to(1000).foreach { _ =>
    val results = graph.vertices().asScala.flatMap { vertex =>
      vertex.vertices(Direction.OUT).asScala.flatMap { vertex =>
        vertex.vertices(Direction.OUT).asScala
      }
    }.size
    assert(results == 327370)
  }

  graph.close
}

object MemoryImpactTp3 extends App {
  val graph = loadGratefulDead(GratefulDead.newGraph)

  1.to(1000).foreach { _ =>
    val results = graph.traversal().V().flatMap { trav =>
      __(trav.get).out().flatMap { trav =>
        __(trav.get).out()
      }: JIterator[Vertex]
    }.toStream.count

    assert(results == 327370)
  }

  graph.close
}