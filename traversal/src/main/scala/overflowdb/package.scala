package object overflowdb {
  implicit def toGraphSugar(graph: OdbGraph): GraphSugar = new GraphSugar(graph)
  implicit def toNodeSugar[N <: Node](node: N): NodeSugar[N] = new NodeSugar[N](node)
  implicit def toElementSugar(element: OdbElement): ElementSugar = new ElementSugar(element)
}
