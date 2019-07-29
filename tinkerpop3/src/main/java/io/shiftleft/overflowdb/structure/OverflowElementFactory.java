package io.shiftleft.overflowdb.structure;

/* To make use of specialized elements (for better memory/performance characteristics), you need to
 * create instances of these factories and register them with OverflowDb. */
public class OverflowElementFactory {

  public interface ForNode<V extends OverflowDbNode> {
    String forLabel();

    V createVertex(Long id, OverflowDb graph);

    V createVertex(NodeRef<V> ref);

    NodeRef<V> createVertexRef(Long id, OverflowDb graph);
  }


  public interface ForEdge<E extends OverflowDbEdge> {
    String forLabel();

    E createEdge(OverflowDb graph, NodeRef outVertex, NodeRef inVertex);
  }

}
