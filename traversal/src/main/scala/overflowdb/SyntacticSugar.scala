package overflowdb

class GraphSugar(val graph: OdbGraph) extends AnyVal {
  def nodeOption(id: Long): Option[Node] =
    Option(graph.node(id))

  def `+`(label: String): Node =
    graph.addNode(label)

  def `+`(id: Long, label: String): Node =
    graph.addNode(id, label)

  def `+`(label: String, properties: Property[_]*): Node =
    graph.addNode(label, keyValuesAsSeq(properties): _*)

  def `+`(label: String, id: Long, properties: Property[_]*): Node =
    graph.addNode(id, label, keyValuesAsSeq(properties): _*)

  private def keyValuesAsSeq(properties: Seq[Property[_]]): Seq[_] = {
    val builder = Seq.newBuilder[Any]
    builder.sizeHint(properties.size * 2)
    properties.foreach { kv =>
      builder += kv.key.name
      builder += kv.value
    }
    builder.result
  }
}

class ElementSugar(val element: OdbElement) extends AnyVal {
  def property[P](propertyKey: PropertyKey[P]): P =
    element.property2(propertyKey.name).asInstanceOf[P]

  def propertyOption[P](propertyKey: PropertyKey[P]): Option[P] =
    Option(property[P](propertyKey))

  // TODO drop suffix `2` after tinkerpop interface is gone
  def setProperty2[P](propertyKeyValue: Property[P]): Unit =
    setProperty2(propertyKeyValue.key, propertyKeyValue.value)

  // TODO drop suffix `2` after tinkerpop interface is gone
  def setProperty2[P](propertyKey: PropertyKey[P], value: P): Unit =
    element.setProperty(propertyKey.name, value)
}

class NodeSugar(val node: Node) extends AnyVal {
  def ---(label: String): SemiEdge =
    new SemiEdge(node, label, Seq.empty)

  def ---(label: String, properties: Property[_]*): SemiEdge =
    new SemiEdge(node, label, properties)
}

private[overflowdb] class SemiEdge(outNode: Node, label: String, properties: Seq[Property[_]]) {
  def -->(inNode: Node): OdbEdge = {
    val keyValues = new Array[Any](properties.size * 2)
    var i: Int = 0
    properties.foreach { case Property(key, value) =>
      keyValues.update(i, key.name)
      i += 1
      keyValues.update(i, value)
      i += 1
    }

    outNode.addEdge2(label, inNode, keyValues: _*)
  }
}

