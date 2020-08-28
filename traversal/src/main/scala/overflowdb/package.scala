package object overflowdb {
  implicit def toPropertyKeyOps[A](propertyKey: PropertyKey[A]): PropertyKeyOps[A] = new PropertyKeyOps[A](propertyKey)
  implicit def toGraphSugar(graph: Graph): GraphSugar = new GraphSugar(graph)
  implicit def toNodeSugar(node: Node): NodeSugar = new NodeSugar(node)
}
