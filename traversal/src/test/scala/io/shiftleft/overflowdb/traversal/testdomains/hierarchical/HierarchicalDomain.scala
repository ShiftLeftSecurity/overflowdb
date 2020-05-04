package io.shiftleft.overflowdb.traversal.testdomains.hierarchical

import io.shiftleft.overflowdb.traversal.{Traversal, TraversalSource, help}
import io.shiftleft.overflowdb.{OdbConfig, OdbGraph}
import java.util

import io.shiftleft.overflowdb.traversal.help.{Doc, TraversalHelp}

/** a simple domain with some hierarchy in the nodes: a top level interface
  * Car
  * Animal (abstract node)
  * Elephant (extends Animal)
  * */
object HierarchicalDomain {
  def newGraph: OdbGraph = newGraph(OdbConfig.withoutOverflow)

  def newGraph(config: OdbConfig): OdbGraph =
    OdbGraph.open(config,
      util.Arrays.asList(Car.factory, Elephant.factory),
      util.Arrays.asList())

  def traversal(graph: OdbGraph) = new HierarchicalDomainTraversalSource(graph)

  lazy val help = new TraversalHelp(getClass.getPackage.getName)
}

@help.TraversalSource
class HierarchicalDomainTraversalSource(graph: OdbGraph) extends TraversalSource(graph) {

  @Doc("all cars")
  def car: Traversal[Car] = label(Car.Label).cast[Car]

  @Doc("all elephants")
  def elephant: Traversal[Elephant] = label(Elephant.Label).cast[Elephant]

  @Doc("all animals")
  def animal: Traversal[Animal] = all.collect { case node: Animal => node }

  lazy val help: String = HierarchicalDomain.help.forTraversalSources
}

