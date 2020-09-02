package overflowdb;

public interface EdgeFactory<E extends Edge> {
  String forLabel();
  E createEdge(Graph graph, NodeRef<NodeDb> outNode, NodeRef<NodeDb> inNode);
}
