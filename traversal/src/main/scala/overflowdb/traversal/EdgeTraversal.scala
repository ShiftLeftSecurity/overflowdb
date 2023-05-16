package overflowdb.traversal

import overflowdb.{Node, Edge}

class EdgeTraversal[E <: Edge](val traversal: Iterator[E]) extends AnyVal {

  /** traverse to outgoing node A ---edge--> [B]
    */
  def outV: Iterator[Node] =
    traversal.map(_.outNode)

  /** traverse to incoming node [A] ---edge--> B
    */
  def inV: Iterator[Node] =
    traversal.map(_.inNode)

}
