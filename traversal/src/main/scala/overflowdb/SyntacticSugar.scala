package overflowdb

import overflowdb.traversal.Traversal

class GraphSugar(val graph: OdbGraph) extends AnyVal {
  def `+`(label: String): Node =
    graph.addNode(label)

  def `+`(label: String, properties: PropertyKeyValue[_]*): Node =
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

class NodeSugar[N <: Node](val node: N) extends AnyVal {
  /** start a new Traversal with this Node, i.e. lift it into a Traversal */
  def start: Traversal[N] =
    Traversal.fromSingle(node)

  def ---(label: String): SemiEdge =
    new SemiEdge(node, label, Seq.empty)

  def ---(label: String, properties: PropertyKeyValue[_]*): SemiEdge =
    new SemiEdge(node, label, properties)
}

private[overflowdb] class SemiEdge(outNode: Node, label: String, properties: Seq[PropertyKeyValue[_]]) {
  def -->(inNode: Node): OdbEdge = {
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

