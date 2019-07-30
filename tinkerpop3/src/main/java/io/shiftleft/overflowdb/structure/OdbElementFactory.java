package io.shiftleft.overflowdb.structure;

/* To make use of specialized elements (for better memory/performance characteristics), you need to
 * create instances of these factories and register them with OverflowDB. */
public class OdbElementFactory {

  public interface ForNode<V extends OdbNode> {
    String forLabel();
    V createVertex(long id, OdbGraph graph);
    V createVertex(NodeRef<V> ref);
    NodeRef<V> createVertexRef(long id, OdbGraph graph);
  }


  public interface ForEdge<E extends OdbEdge> {
    String forLabel();

    E createEdge(OdbGraph graph, NodeRef outVertex, NodeRef inVertex);
  }

}
