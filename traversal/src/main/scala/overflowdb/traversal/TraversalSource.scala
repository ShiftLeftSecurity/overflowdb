package overflowdb.traversal

import overflowdb.{Node, OdbGraph}

abstract class TraversalSource(graph: OdbGraph) {
  def all: Traversal[Node] =
    Traversal(graph.nodes())

  def id(id: Long): Traversal[Node] =
    Traversal(graph.node(id))

  def idTyped[A <: Node](id: Long): Traversal[A] =
    Traversal(graph.node(id)).cast[A]

  def ids(ids: Long*): Traversal[Node] =
    Traversal(graph.nodes(ids: _*))

  def label(label: String): Traversal[Node] =
    Traversal(graph.nodes(label))
}
