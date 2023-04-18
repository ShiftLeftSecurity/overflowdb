package overflowdb.traversal

import overflowdb.{Node, Graph, Property}
import scala.jdk.CollectionConverters._

class TraversalSource(graph: Graph) {
  def all: Traversal[Node] =
    Traversal(graph.nodes())

  def id[NodeType: DefaultsToNode](id: Long): Traversal[NodeType] =
    Traversal(graph.node(id)).cast[NodeType]

  def ids[NodeType: DefaultsToNode](ids: Long*): Traversal[NodeType] =
    Traversal(graph.nodes(ids: _*)).cast[NodeType]

  def label(label: String): Traversal[Node] =
    Traversal(graph.nodes(label))

  def hasLabel(label: String): Traversal[Node] =
    this.label(label)

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
      Traversal.from(graph.nodes()).has(key, value)
    }
  }

  /** Start traversal with all nodes with given label that have given property value Inspects the cardinality of the
    * indices of the properties and labels, and takes the smaller one
    */
  def labelAndProperty(label: String, property: Property[_]): Traversal[Node] =
    this.labelAndProperty(label, property.key.name, property.value)

  /** Start traversal with all nodes with given label that have given property value Inspects the cardinality of the
    * indices of the properties and labels, and takes the smaller one
    */
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
  def apply(graph: Graph): TraversalSource = new TraversalSource(graph)
}
