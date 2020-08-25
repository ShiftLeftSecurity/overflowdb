package overflowdb.traversal

import overflowdb.{Node, OdbEdgeTp3}

class EdgeTraversal[E <: OdbEdgeTp3](val traversal: Traversal[E]) extends AnyVal {

  /** traverse to outgoing node
   * A ---edge--> [B]
   * */
  def outV: Traversal[Node] =
    traversal.map(_.outVertex.asInstanceOf[Node])

  /** traverse to incoming node
   * [A] ---edge--> B
   * */
  def inV: Traversal[Node] =
    traversal.map(_.inVertex.asInstanceOf[Node])

}
