package overflowdb.traversal

import overflowdb.{Node, OdbGraph, Property}
import scala.jdk.CollectionConverters._

class TraversalSource(graph: OdbGraph) {
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

  def labelTyped[A <: Node](label: String): Traversal[A] =
    this.label(label).cast[A]

  /** Start traversal with all nodes that have given property value */
  def has(property: Property[_]): Traversal[Node] = {
    this.has(property.key.name, property.value)
  }

  /** Start traversal with all nodes that have given property value */
  def has(key: String, value: Any): Traversal[Node] = {
    if (graph.indexManager.isIndexed(key)) {
      val nodes = graph.indexManager.lookup(key, value)
      Traversal.from(nodes.asScala)
    } else {
      // maybe print a warning: may make sense to create an index
      Traversal(graph.nodes().has(key, value))
    }
  }
}

object TraversalSource {
  def apply(graph: OdbGraph): TraversalSource = new TraversalSource(graph)
}
