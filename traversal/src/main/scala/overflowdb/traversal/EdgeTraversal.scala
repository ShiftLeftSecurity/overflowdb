package overflowdb.traversal

import overflowdb.{NodeRef, OdbEdge}

class EdgeTraversal[A <: OdbEdge](val traversal: Traversal[A]) extends AnyVal {

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
