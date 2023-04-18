package overflowdb.traversal

import overflowdb.{Graph, Node, Property}

import scala.jdk.CollectionConverters._

class TraversalSource(graph: Graph) {
  type Traversal[+A] = Iterator[A]
  import ImplicitsTmp._
  def all: Traversal[Node] =
    graph.nodes().asScala

  def id[NodeType: DefaultsToNode](id: Long): Traversal[NodeType] =
    Option(graph.node(id)).iterator.asInstanceOf[Traversal[NodeType]]

  def ids[NodeType: DefaultsToNode](ids: Long*): Traversal[NodeType] =
    graph.nodes(ids: _*).asScala.asInstanceOf[Traversal[NodeType]]

  def label(label: String): Traversal[Node] =
    graph.nodes(label).asScala

  def hasLabel(label: String): Traversal[Node] =
    this.label(label)

  def labelTyped[A <: Node](label: String): Traversal[A] =
    this.label(label).asInstanceOf[Traversal[A]]

  /** Start traversal with all nodes that have given property value */
  def has(property: Property[_]): Traversal[Node] = {
    this.has(property.key.name, property.value)
  }

  /** Start traversal with all nodes that have given property value */
  def has(key: String, value: Any): Traversal[Node] = {
    if (graph.indexManager.isIndexed(key)) {
      val nodes = graph.indexManager.lookup(key, value)
      nodes.asScala.iterator
    } else {
      // maybe print a warning: may make sense to create an index
      new ElementTraversal(graph.nodes().asScala.iterator).has(key, value)
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
      new ElementTraversal(nodesByPropertyIndex.asScala.iterator).label(label)
    else
      new ElementTraversal(this.label(label)).has(propertyKey, propertyValue)
  }
}

object TraversalSource {
  def apply(graph: Graph): TraversalSource = new TraversalSource(graph)
}
