package overflowdb.algorithm

import overflowdb.traversal.Traversal
import overflowdb.{Direction, Edge, Node}

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
          val edgesBetween0: PathEntry = edgesBetween(nodeA, nodeB) match {
            case Nil => throw new AssertionError(s"no edges between nodes $nodeA and $nodeB - this looks like a bug in PathFinder")
            case Seq(edgeEntry) => edgeEntry
            case multipleEdges => EdgeEntries(multipleEdges)
          }
          Seq(
            NodeEntry(nodeA),
            edgesBetween0,
            NodeEntry (nodeB),
          )
        }.to(Seq)
      )
    }
  }

  private def edgesBetween(nodeA: Node, nodeB: Node): Seq[EdgeEntry] = {
    ???
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
