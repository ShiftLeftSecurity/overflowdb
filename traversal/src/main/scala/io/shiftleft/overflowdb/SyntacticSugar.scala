package io.shiftleft.overflowdb

import io.shiftleft.overflowdb.traversal.PropertyKeyValue

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
    new SemiEdge(node, label)

}

private[overflowdb] class SemiEdge(outNode: NodeRef[_], label: String) {
  def -->(inNode: NodeRef[_]): OdbEdge =
    outNode.addEdge(label, inNode).asInstanceOf[OdbEdge]
}

