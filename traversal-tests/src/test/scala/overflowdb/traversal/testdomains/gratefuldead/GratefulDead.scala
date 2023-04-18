package overflowdb.traversal.testdomains.gratefuldead

import overflowdb.formats.graphml.GraphMLImporter
import java.util
import overflowdb.traversal.TraversalSource
import overflowdb.{Config, Graph}
import overflowdb.traversal._

/* visual schema: https://raw.githubusercontent.com/apache/tinkerpop/347eb9a8231c48aa22eced9b07dd6241305898c6/docs/static/images/grateful-dead-schema.png */
object GratefulDead {
  def newGraph: Graph = newGraph(Config.withoutOverflow)

  def newGraph(config: Config): Graph =
    Graph.open(
      config,
      util.Arrays.asList(Song.factory, Artist.factory),
      util.Arrays.asList(FollowedBy.factory, SungBy.factory, WrittenBy.factory)
    )

  def newGraphWithData: Graph = {
    val graph = newGraph
    loadData(graph)
    graph
  }

  def loadData(graph: Graph): Unit =
    GraphMLImporter.runImport(graph, "src/test/resources/grateful-dead.xml")

  def traversal(graph: Graph) = new GratefulDeadTraversalSource(graph)
}

class GratefulDeadTraversalSource(graph: Graph) extends TraversalSource(graph) {
  def artists: Traversal[Artist] = label(Artist.Label).cast[Artist]
  def songs: Traversal[Song] = label(Song.Label).cast[Song]
}
