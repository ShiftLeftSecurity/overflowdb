package overflowdb

class GraphSugar(val graph: OdbGraph) extends AnyVal {
  def `+`(label: String): NodeRef[_] =
    graph.addNode(label)

  def `+`(label: String, properties: PropertyKeyValue[_]*): NodeRef[_] =
    graph.addNode(label, keyValuesAsSeq(properties): _*)

  private def keyValuesAsSeq(properties: Seq[PropertyKeyValue[_]]): Seq[_] = {
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
    element.property2[P](propertyKey.name)

  def propertyOption[P](propertyKey: PropertyKey[P]): Option[P] =
    Option(property[P](propertyKey))

  // TODO drop suffix `2` after tinkerpop interface is gone
  def setProperty2[P](propertyKeyValue: PropertyKeyValue[P]): Unit =
    setProperty2(propertyKeyValue.key, propertyKeyValue.value)

  // TODO drop suffix `2` after tinkerpop interface is gone
  def setProperty2[P](propertyKey: PropertyKey[P], value: P): Unit =
    element.setProperty(propertyKey.name, value)
}

class NodeRefSugar(val node: NodeRef[_]) extends AnyVal {
  def ---(label: String): SemiEdge =
    new SemiEdge(node, label, Seq.empty)

  def ---(label: String, properties: PropertyKeyValue[_]*): SemiEdge =
    new SemiEdge(node, label, properties)
}

private[overflowdb] class SemiEdge(outNode: NodeRef[_], label: String, properties: Seq[PropertyKeyValue[_]]) {
  def -->(inNode: NodeRef[_]): OdbEdge = {
    val tinkerpopKeyValues = new Array[Any](properties.size * 2)
    var i: Int = 0
    properties.foreach { case PropertyKeyValue(key, value) =>
      tinkerpopKeyValues.update(i, key.name)
      i += 1
      tinkerpopKeyValues.update(i, value)
      i += 1
    }

    outNode.addEdge(label, inNode, tinkerpopKeyValues: _*).asInstanceOf[OdbEdge]
  }
}

