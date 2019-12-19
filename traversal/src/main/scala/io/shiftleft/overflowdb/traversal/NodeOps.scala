package io.shiftleft.overflowdb.traversal

import io.shiftleft.overflowdb.OdbNode
import org.apache.tinkerpop.gremlin.structure.Direction

trait NodeOps { this: OdbNode =>
  def adjacentNodes[A](direction: Direction, label: String): Traversal[A] =
    Traversal(vertices(direction, label)).cast[A]
}
