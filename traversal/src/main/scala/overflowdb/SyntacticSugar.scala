package overflowdb

class PropertyPredicate[A](val key: PropertyKey[A], val predicate: A => Boolean)

class PropertyKeyOps[A](val propertyKey: PropertyKey[A]) extends AnyVal {
  def where(predicate: A => Boolean): PropertyPredicate[A] =
    new PropertyPredicate[A](propertyKey, predicate)
}

class GraphSugar(val graph: Graph) extends AnyVal {
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

class NodeSugar(val node: Node) extends AnyVal {
  def ---(label: String): SemiEdge =
    new SemiEdge(node, label, Seq.empty)

  def ---(label: String, properties: Property[_]*): SemiEdge =
    new SemiEdge(node, label, properties)
}

private[overflowdb] class SemiEdge(outNode: Node, label: String, properties: Seq[Property[_]]) {
  def -->(inNode: Node): Edge = {
    val keyValues = new Array[Any](properties.size * 2)
    var i: Int = 0
    properties.foreach { property =>
      keyValues.update(i, property.key.name)
      i += 1
      keyValues.update(i, property.value)
      i += 1
    }

    outNode.addEdge(label, inNode, keyValues: _*)
  }
}

