package io.shiftleft.overflowdb.traversal.testdomains.simple

import io.shiftleft.overflowdb.traversal.{Traversal, TraversalSource}
import io.shiftleft.overflowdb.{OdbConfig, OdbGraph}
import java.util

/** probably the most simple domain one can think of:
 * Thing --- Connection --> Thing
 * */
object SimpleDomain {
  def newGraph: OdbGraph = newGraph(OdbConfig.withoutOverflow)

  def newGraph(config: OdbConfig): OdbGraph =
    OdbGraph.open(config,
      util.Arrays.asList(Thing.factory),
      util.Arrays.asList(Connection.factory))

  def traversal(graph: OdbGraph) = new SimpleDomainTraversalSource(graph)
}

class SimpleDomainTraversalSource(graph: OdbGraph) extends TraversalSource(graph) {
  def things: Traversal[Thing] = label(Thing.Label).cast[Thing]
}
