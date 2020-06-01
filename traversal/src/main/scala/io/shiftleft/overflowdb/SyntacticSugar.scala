package io.shiftleft.overflowdb

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

class NodeRefSugar(val node: NodeRef[_]) extends AnyVal {

  def ---(label: String): SemiEdge =
    new SemiEdge(node, label, Seq.empty)

  def ---(label: String, properties: PropertyKeyValue[_]*): SemiEdge =
    new SemiEdge(node, label, properties)

}

private[overflowdb] class SemiEdge(outNode: NodeRef[_], label: String, properties: Seq[PropertyKeyValue[_]]) {
  def -->(inNode: NodeRef[_]): OdbEdge = {
    val tinkerpopProps = new Array[AnyRef](properties.size * 2)
    outNode.addEdge(label, inNode).asInstanceOf[OdbEdge]
  }
}

