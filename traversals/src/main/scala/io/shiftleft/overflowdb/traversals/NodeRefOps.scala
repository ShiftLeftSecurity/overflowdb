package io.shiftleft.overflowdb.traversals

import io.shiftleft.overflowdb.NodeRef
import org.apache.tinkerpop.gremlin.structure.Direction

trait NodeRefOps[Ref <: NodeRef[_]] { this: Ref =>
  def adjacentNodes[A](direction: Direction, label: String): Traversal[A] =
    new Traversal(vertices(direction, label)).cast[A]

  /** lift this NodeRef into a Traversal */
  def start: Traversal[Ref] =
    new Traversal[Ref](Traversal.fromSingle(this))
}
