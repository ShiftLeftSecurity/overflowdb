package overflowdb.traversal.testdomains.hierarchical

import overflowdb.traversal.{TraversalSource, help}
import overflowdb.{Config, Graph}
import overflowdb.traversal._
import overflowdb.traversal.help.Table.AvailableWidthProvider

import java.util
import overflowdb.traversal.help.{Doc, DocSearchPackages, TraversalHelp}
import overflowdb.traversal.testdomains.simple.SimpleDomain.getClass

/** a simple domain with some hierarchy in the nodes: a top level interface Car Animal (abstract node) Elephant (extends
  * Animal)
  */
object HierarchicalDomain {
  implicit val defaultDocSearchPackage: DocSearchPackages =
    DocSearchPackages(getClass.getPackage.getName)

  lazy val help = new TraversalHelp(defaultDocSearchPackage)

  def newGraph: Graph = newGraph(Config.withoutOverflow)

  def newGraph(config: Config): Graph =
    Graph.open(config, util.Arrays.asList(Car.factory, Elephant.factory), util.Arrays.asList())

  def traversal(graph: Graph) = new HierarchicalDomainTraversalSource(graph)
}

@help.TraversalSource
class HierarchicalDomainTraversalSource(graph: Graph) extends TraversalSource(graph) {

  @Doc(info = "all cars")
  def car: Traversal[Car] = label(Car.Label).cast[Car]

  @Doc(info = "all elephants")
  def elephant: Traversal[Elephant] = label(Elephant.Label).cast[Elephant]

  @Doc(info = "all animals")
  def animal: Traversal[Animal] = all.collect { case node: Animal => node }

  def help(implicit availableWidthProvider: AvailableWidthProvider): String =
    HierarchicalDomain.help.forTraversalSources
}
