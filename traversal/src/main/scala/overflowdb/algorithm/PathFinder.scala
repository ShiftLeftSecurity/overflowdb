package overflowdb.algorithm

import overflowdb.traversal.Traversal
import overflowdb.{Direction, Node}

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

      // TODO use for comprehension
      PathWithEdges(nodes.sliding(2).flatMap { case Seq(nodeA, nodeB) =>
        edgesBetween(nodeA, nodeB).map { edgeEntry =>
          Seq(
            NodeEntry(nodeA),
            edgeEntry,
            NodeEntry (nodeB),
          )
        }
      } )
      ???
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
  case class EdgeEntry(direction: Direction, label: String) extends PathEntry {
    assert(direction == Direction.IN || direction == Direction.OUT,
      s"direction must be either IN or OUT, but was $direction")
  }


}
