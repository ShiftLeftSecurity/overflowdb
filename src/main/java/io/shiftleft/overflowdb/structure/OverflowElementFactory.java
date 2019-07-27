package io.shiftleft.overflowdb.structure;

/* To make use of specialized elements (for better memory/performance characteristics), you need to
 * create instances of these factories and register them with TinkerGraph. */
public class OverflowElementFactory {

  public interface ForNode<V extends OverflowDbNode> {
    String forLabel();

    V createVertex(Long id, TinkerGraph graph);

    V createVertex(VertexRef<V> ref);

    VertexRef<V> createVertexRef(Long id, TinkerGraph graph);
  }


  public interface ForEdge<E extends OverflowDbEdge> {
    String forLabel();

    E createEdge(TinkerGraph graph, VertexRef outVertex, VertexRef inVertex);
  }

}
