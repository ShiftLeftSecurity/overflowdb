package io.shiftleft.overflowdb.traversal

import io.shiftleft.overflowdb.{NodeRef, OdbEdge}

class EdgeTraversal[E <: OdbEdge](val traversal: Traversal[E]) extends AnyVal {

  /** traverse to outgoing node
   * A ---edge--> [B]
   * */
  def outV: Traversal[NodeRef[_]] =
    traversal.map(_.outVertex.asInstanceOf[NodeRef[_]])

  /** traverse to incoming node
   * [A] ---edge--> B
   * */
  def inV: Traversal[NodeRef[_]] =
    traversal.map(_.inVertex.asInstanceOf[NodeRef[_]])

}
