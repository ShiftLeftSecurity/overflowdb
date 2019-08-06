package io.shiftleft.overflowdb.structure;

public abstract class NodeFactory<V extends OdbNode> {
  public abstract String forLabel();
  public abstract V createNode(NodeRef<V> ref);
  public abstract NodeRef<V> createNodeRef(OdbGraph graph, long id);

  public V createNode(OdbGraph graph, long id) {
    final NodeRef<V> ref = createNodeRef(graph, id);
    final V node = createNode(ref);
    ref.setNode(node);
    return node;
  }
}

