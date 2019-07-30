package io.shiftleft.overflowdb.structure;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class NodeRefWithLabel<N extends OverflowDbNode> extends NodeRef<N> {
  private final String label;

  public NodeRefWithLabel(final Object vertexId,
                          final Graph graph,
                          N node,
                          final String label) {
    super(vertexId, graph, node);
    this.label = label;
  }

  @Override
  public String label() {
    return label;
  }
}
