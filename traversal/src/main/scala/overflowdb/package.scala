package object overflowdb {
  implicit def toGraphSugar(graph: OdbGraph): GraphSugar = new GraphSugar(graph)
  implicit def toNodeSugar(node: Node): NodeSugar = new NodeSugar(node)
  implicit def toElementSugar(element: OdbElement): ElementSugar = new ElementSugar(element)
}
