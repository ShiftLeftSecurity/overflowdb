package io.shiftleft.overflowdb.structure;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class VertexRefWithLabel<V extends Vertex> extends VertexRef<V> {
  private final String label;

  public VertexRefWithLabel(final Object vertexId,
                            final Graph graph,
                            V vertex,
                            final String label) {
    super(vertexId, graph, vertex);
    this.label = label;
  }

  @Override
  public String label() {
    return label;
  }
}
