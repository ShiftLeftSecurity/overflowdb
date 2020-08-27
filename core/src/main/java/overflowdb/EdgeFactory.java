package overflowdb;

public interface EdgeFactory<E extends Edge> {
  String forLabel();
  E createEdge(OdbGraph graph, NodeRef<OdbNode> outNode, NodeRef<OdbNode> inNode);
}
