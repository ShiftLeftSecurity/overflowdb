package io.shiftleft.overflowdb.traversal.testdomains.simple

import io.shiftleft.overflowdb.traversal.{Traversal, TraversalSource, help}
import io.shiftleft.overflowdb.{OdbConfig, OdbGraph}
import java.util

import io.shiftleft.overflowdb.traversal.help.{Doc, TraversalHelp}

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

  lazy val help = new TraversalHelp(getClass.getPackage.getName)
}

@help.TraversalSource
class SimpleDomainTraversalSource(graph: OdbGraph) extends TraversalSource(graph) {

  @Doc("all things")
  def things: Traversal[Thing] = label(Thing.Label).cast[Thing]

  lazy val help: String = SimpleDomain.help.forSources
}

