package io.shiftleft.overflowdb.structure;

public interface EdgeFactory<E extends OdbEdge> {
  String forLabel();
  E createEdge(OdbGraph graph, NodeRef outNode, NodeRef inNode);
}
