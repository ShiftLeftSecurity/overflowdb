package overflowdb.algorithm

import overflowdb.traversal.Traversal
import overflowdb.{Direction, Edge, Node}

import scala.jdk.CollectionConverters.IteratorHasAsScala

object PathFinder {
  def apply(nodeA: Node, nodeB: Node): Seq[Path] = {
    if (nodeA == nodeB) Seq(Path(Seq(nodeA)))
    else {
      Traversal.fromSingle(nodeA)
        .enablePathTracking
        .repeat(_.both)(_.emit.times(2))
        .path
        .foreach(println)

      ???
    }
  }

  case class Path(nodes: Seq[Node]) {
    def withEdges: PathWithEdges = {
      // TODO use for comprehension?
      PathWithEdges(
        nodes.sliding(2).flatMap { case Seq(nodeA, nodeB) =>
          val edgesBetweenAsPathEntry: PathEntry = edgesBetween(nodeA, nodeB) match {
            case Nil => throw new AssertionError(s"no edges between nodes $nodeA and $nodeB - this looks like a bug in PathFinder")
            case Seq(edgeEntry) => edgeEntry
            case multipleEdges => EdgeEntries(multipleEdges)
          }
          Seq(
            NodeEntry(nodeA),
            edgesBetweenAsPathEntry,
            NodeEntry (nodeB),
          )
        }.to(Seq)
      )
    }
  }

  private def edgesBetween(nodeA: Node, nodeB: Node): Seq[EdgeEntry] = {
    val outEdges = nodeA.outE.asScala.filter(_.inNode == nodeB).map(edge => EdgeEntry(Direction.OUT, edge.label))
    val inEdges  = nodeA.inE.asScala.filter(_.outNode == nodeB).map(edge => EdgeEntry(Direction.IN, edge.label))
    outEdges.to(Seq) ++ inEdges.to(Seq)
  }

  case class PathWithEdges(elements: Seq[PathEntry])
  sealed trait PathEntry
  case class NodeEntry(node: Node) extends PathEntry {
    def label: String = node.label()
    def id: Long = node.id()
  }
  case class EdgeEntries(edgeEntries: Seq[EdgeEntry]) extends PathEntry
  case class EdgeEntry(direction: Direction, label: String) extends PathEntry {
    assert(direction == Direction.IN || direction == Direction.OUT,
      s"direction must be either IN or OUT, but was $direction")
  }


}
