package overflowdb.traversal.testdomains.hierarchical

import overflowdb.traversal.{Traversal, TraversalSource, help}
import overflowdb.{Config, Graph}
import java.util

import overflowdb.traversal.help.{Doc, TraversalHelp}

/** a simple domain with some hierarchy in the nodes: a top level interface
  * Car
  * Animal (abstract node)
  * Elephant (extends Animal)
  * */
object HierarchicalDomain {
  def newGraph: Graph = newGraph(Config.withoutOverflow)

  def newGraph(config: Config): Graph =
    Graph.open(config,
      util.Arrays.asList(Car.factory, Elephant.factory),
      util.Arrays.asList())

  def traversal(graph: Graph) = new HierarchicalDomainTraversalSource(graph)

  lazy val help = new TraversalHelp(getClass.getPackage.getName)
}

@help.TraversalSource
class HierarchicalDomainTraversalSource(graph: Graph) extends TraversalSource(graph) {

  @Doc("all cars")
  def car: Traversal[Car] = label(Car.Label).cast[Car]

  @Doc("all elephants")
  def elephant: Traversal[Elephant] = label(Elephant.Label).cast[Elephant]

  @Doc("all animals")
  def animal: Traversal[Animal] = all.collect { case node: Animal => node }

  lazy val help: String = HierarchicalDomain.help.forTraversalSources
}

