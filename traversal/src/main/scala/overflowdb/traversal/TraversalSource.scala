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

  /** Start traversal with all nodes with given label that have given property value
   * Inspects the cardinality of the indices of the properties and labels, and takes the smaller one */
  def labelAndProperty(label: String, property: Property[_]): Traversal[Node] =
    this.labelAndProperty(label, property.key.name, property.value)

  /** Start traversal with all nodes with given label that have given property value
   * Inspects the cardinality of the indices of the properties and labels, and takes the smaller one */
  def labelAndProperty(label: String, propertyKey: String, propertyValue: Any): Traversal[Node] = {
    lazy val propertyIsIndexed = graph.indexManager.isIndexed(propertyKey)
    lazy val nodesByPropertyIndex = graph.indexManager.lookup(propertyKey, propertyValue)
    lazy val cardinalityByLabel = graph.nodeCount(label)

    if (propertyIsIndexed && nodesByPropertyIndex.size <= cardinalityByLabel)
      Traversal.from(nodesByPropertyIndex.asScala).label(label)
    else
      this.label(label).has(propertyKey, propertyValue)
  }
}

object TraversalSource {
  def apply(graph: OdbGraph): TraversalSource = new TraversalSource(graph)
}
