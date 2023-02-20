package overflowdb;

public abstract class NodeFactory<V extends NodeDb> {
  public abstract String forLabel();

  public abstract V createNode(NodeRef<V> ref);

  public abstract NodeRef<V> createNodeRef(Graph graph, long id);

  public  V createNode(Graph graph, long id) {
    return createNode(graph, id, null);
  }

  public V createNode(Graph graph, long id, NodeRef<V> ref) {
    if(ref == null) ref = createNodeRef(graph, id);
    final V node = createNode(ref);
    node.markAsDirty(); //freshly created, i.e. not yet serialized
    ref.setNode(node);
    return node;
  }

  public DetachedNodeData createDetached(){
    return new DetachedNodeGeneric(forLabel());
  }

}

