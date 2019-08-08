package io.shiftleft.overflowdb;

public interface EdgeFactory<E extends OdbEdge> {
  String forLabel();
  E createEdge(OdbGraph graph, NodeRef<OdbNode> outNode, NodeRef<OdbNode> inNode);
}
