package overflowdb.traversal.testdomains.simple

import overflowdb.traversal.{Traversal, TraversalSource, help}
import overflowdb.{Config, Graph}

import java.util
import overflowdb.traversal.help.{Doc, DocSearchPackages, TraversalHelp}

/** probably the most simple domain one can think of:
 * Thing --- Connection --> Thing
 * */
object SimpleDomain {
  implicit val defaultDocSearchPackage: DocSearchPackages = DocSearchPackages(Seq("custom3"))

  def newGraph: Graph = newGraph(Config.withoutOverflow)

  def newGraph(config: Config): Graph =
    Graph.open(config,
      util.Arrays.asList(Thing.factory),
      util.Arrays.asList(Connection.factory))

  def traversal(graph: Graph) = new SimpleDomainTraversalSource(graph)

  lazy val help = new TraversalHelp(getClass.getPackage.getName)
}

@help.TraversalSource
class SimpleDomainTraversalSource(graph: Graph) extends TraversalSource(graph) {

  @Doc(info = "all things")
  def things: Traversal[Thing] = label(Thing.Label).cast[Thing]

  lazy val help: String = SimpleDomain.help.forTraversalSources
}

