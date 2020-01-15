package io.shiftleft.overflowdb.traversal

import io.shiftleft.overflowdb.{NodeRef, OdbEdge}

class EdgeTraversal[A <: OdbEdge](val traversal: Traversal[A]) extends AnyVal {
  def inV: Traversal[NodeRef[_]] =
    traversal.map(_.inVertex.asInstanceOf[NodeRef[_]])
}
