package io.shiftleft.overflowdb.traversal.testdomains.gratefuldead

import java.util

import io.shiftleft.overflowdb.traversal.{Traversal, TraversalSource}
import io.shiftleft.overflowdb.{OdbConfig, OdbGraph}
import org.apache.tinkerpop.gremlin.structure.io.IoCore

/* visual schema: https://raw.githubusercontent.com/apache/tinkerpop/347eb9a8231c48aa22eced9b07dd6241305898c6/docs/static/images/grateful-dead-schema.png */
object GratefulDead {
  def newGraph: OdbGraph = newGraph(OdbConfig.withoutOverflow)

  def newGraph(config: OdbConfig): OdbGraph =
    OdbGraph.open(config,
      util.Arrays.asList(Song.factory, Artist.factory),
      util.Arrays.asList(FollowedBy.factory, SungBy.factory, WrittenBy.factory))

  def newGraphWithData: OdbGraph = {
    val graph = newGraph
    loadData(graph)
    graph
  }

  def loadData(graph: OdbGraph): Unit =
    graph.traversal().io("src/test/resources/grateful-dead.xml").read().iterate()

  def traversal(graph: OdbGraph) = new GratefulDeadTraversalSource(graph)
}

class GratefulDeadTraversalSource(graph: OdbGraph) extends TraversalSource(graph) {
  def artists: Traversal[Artist] = labelTyped(Artist.Label)
  def songs: Traversal[Song] = labelTyped(Song.Label)
}