package overflowdb.traversal

import overflowdb.NodeRef
import org.apache.tinkerpop.gremlin.structure.Direction

trait NodeRefOps[Ref <: NodeRef[_]] { this: Ref =>
  def adjacentNodes[A](direction: Direction, label: String): Traversal[A] =
    Traversal(vertices(direction, label)).cast[A]

  /** lift this NodeRef into a Traversal */
  def start: Traversal[Ref] =
    Traversal.fromSingle(this)
}
