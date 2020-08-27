package overflowdb.traversal

import overflowdb.{Node, OdbEdge}

class EdgeTraversal[E <: OdbEdge](val traversal: Traversal[E]) extends AnyVal {

  /** traverse to outgoing node
   * A ---edge--> [B]
   * */
  def outV: Traversal[Node] =
    traversal.map(_.outNode)

  /** traverse to incoming node
   * [A] ---edge--> B
   * */
  def inV: Traversal[Node] =
    traversal.map(_.inNode)

}
