package io.shiftleft.overflowdb.testdomains.gratefuldead

import java.util

import io.shiftleft.overflowdb.{OdbConfig, OdbGraph}
import org.apache.tinkerpop.gremlin.structure.io.IoCore

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
    graph.io(IoCore.graphml).readGraph("src/test/resources/grateful-dead.xml")

  def traversal(graph: OdbGraph) = new GratefulDeadTraversalSource(graph)
}