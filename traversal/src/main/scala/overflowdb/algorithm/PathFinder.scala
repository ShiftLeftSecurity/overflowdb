package overflowdb.algorithm

import overflowdb.{Direction, Node}
import overflowdb.traversal._

import scala.jdk.CollectionConverters.IteratorHasAsScala

object PathFinder {
  def apply(nodeA: Node, nodeB: Node, maxDepth: Int = -1): Seq[Path] = {
    if (nodeA == nodeB) Seq(Path(Seq(nodeA)))
    else {
      Traversal
        .fromSingle(nodeA)
        .enablePathTracking
        .repeat(_.both) { initialBehaviour =>
          val behaviour = initialBehaviour.dedup // no cycles
            .until(_.is(nodeB)) // don't continue on a given path if we've reached our destination
          if (maxDepth > -1) behaviour.maxDepth(maxDepth)
          else behaviour
        }
        .is(nodeB) // we only care about the paths that lead to our destination
        .path
        .cast[Seq[Node]]
        .map(Path.apply)
        .toSeq
    }
  }

  case class Path(nodes: Seq[Node]) {
    def withEdges: PathWithEdges = {
      val elements = Seq.newBuilder[PathEntry]
      nodes.headOption.foreach { firstElement =>
        elements.addOne(NodeEntry(firstElement))
      }

      for {
        case Seq(nodeA, nodeB) <- nodes.sliding(2)
        edgesBetweenAsPathEntry: PathEntry =
          edgesBetween(nodeA, nodeB) match {
            case Nil =>
              throw new AssertionError(
                s"no edges between nodes $nodeA and $nodeB - this looks like a bug in PathFinder"
              )
            case Seq(edgeEntry) => edgeEntry
            case multipleEdges  => EdgeEntries(multipleEdges)
          }
      } {
        elements.addOne(edgesBetweenAsPathEntry)
        elements.addOne(NodeEntry(nodeB))
      }

      PathWithEdges(elements.result())
    }
  }

  private def edgesBetween(nodeA: Node, nodeB: Node): Seq[EdgeEntry] = {
    val outEdges = nodeA.outE.asScala.filter(_.inNode == nodeB).map(edge => EdgeEntry(Direction.OUT, edge.label))
    val inEdges = nodeA.inE.asScala.filter(_.outNode == nodeB).map(edge => EdgeEntry(Direction.IN, edge.label))
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
    assert(
      direction == Direction.IN || direction == Direction.OUT,
      s"direction must be either IN or OUT, but was $direction"
    )
  }

}
