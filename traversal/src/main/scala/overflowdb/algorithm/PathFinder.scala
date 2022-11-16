package overflowdb.algorithm

import overflowdb.traversal.Traversal
import overflowdb.{Direction, Node}

object PathFinder {
  case class Path(elements: Seq[PathEntry])

  def apply(nodeA: Node, nodeB: Node): Seq[Path] = {
    if (nodeA == nodeB) Seq(Path(Seq(NodeEntry(nodeA))))
    else {
      Traversal.fromSingle(nodeA)
        .enablePathTracking
        .repeat(_.union(
          _.outE.inV,
          _.inE.outV
        ))(_.emit.times(2))
        .path
        .foreach(println)

      ???
    }
  }

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
