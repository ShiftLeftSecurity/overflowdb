package object overflowdb {
  implicit def toGraphSugar(graph: Graph): GraphSugar = new GraphSugar(graph)
  implicit def toNodeSugar(node: Node): NodeSugar = new NodeSugar(node)
  implicit def toElementSugar(element: Element): ElementSugar = new ElementSugar(element)
}
