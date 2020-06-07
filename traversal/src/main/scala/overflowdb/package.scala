package object overflowdb {
  implicit def toGraphSugar(graph: OdbGraph): GraphSugar = new GraphSugar(graph)
  implicit def toNodeRefSugar(node: NodeRef[_]): NodeRefSugar = new NodeRefSugar(node)
  implicit def toElementSugar(element: OdbElement): ElementSugar = new ElementSugar(element)
}
