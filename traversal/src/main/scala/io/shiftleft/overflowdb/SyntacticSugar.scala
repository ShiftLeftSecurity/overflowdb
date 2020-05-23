package io.shiftleft.overflowdb

class GraphSugar(val wrapped: OdbGraph) extends AnyVal {
  def `+`(label: String): NodeRef[_] =
    wrapped.addNode(label)
}

class NodeRefSugar(val wrapped: NodeRef[_]) extends AnyVal {
  def ---(label: String): SemiEdge =
    new SemiEdge(wrapped, label)
}

class SemiEdge(outNode: NodeRef[_], label: String) {
  def -->(inNode: NodeRef[_]): OdbEdge =
    outNode.addEdge(label, inNode).asInstanceOf[OdbEdge]
}

