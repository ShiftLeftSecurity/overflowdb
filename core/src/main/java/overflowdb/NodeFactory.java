package overflowdb;

public abstract class NodeFactory<V extends NodeDb> {
  public abstract String forLabel();

  /** unique id for this node's label
   *  This is mostly an optimization for storage - we could as well serialize labels as string, but numbers are more efficient.
   *  Since we know our schema at compile time, we can assign unique ids for each label.
   *  */
  public abstract int forLabelId();

  public abstract V createNode(NodeRef<V> ref);

  public abstract NodeRef<V> createNodeRef(Graph graph, long id);

  public abstract NodeLayoutInformation layoutInformation();

  public V createNode(Graph graph, long id) {
    final NodeRef<V> ref = createNodeRef(graph, id);
    final V node = createNode(ref);
    node.markAsDirty(); //freshly created, i.e. not yet serialized
    ref.setNode(node);
    return node;
  }

}

