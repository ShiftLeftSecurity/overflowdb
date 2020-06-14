package overflowdb.traversal

import overflowdb.{Node, OdbGraph}

// TODO stop using `cast` step once we have our own core api
abstract class TraversalSource(graph: OdbGraph) {
  def all: Traversal[Node] =
    Traversal(graph.vertices()).cast[Node]

  def id(id: Long): Traversal[Node] =
    Traversal(graph.vertices(id)).cast[Node]

  def idTyped[A <: Node](id: Long): Traversal[A] =
    Traversal(graph.vertices(id)).cast[A]

  def ids(ids: Long*): Traversal[Node] =
    Traversal(graph.vertices(ids: _*)).cast[Node]

  def label(label: String): Traversal[Node] =
    Traversal(graph.nodesByLabel(label))
}
