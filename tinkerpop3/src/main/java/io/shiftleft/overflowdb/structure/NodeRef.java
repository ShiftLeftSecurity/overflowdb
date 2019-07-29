package io.shiftleft.overflowdb.structure;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.io.IOException;
import java.util.Iterator;

public abstract class NodeRef<V extends Vertex> extends ElementRef<V> implements Vertex {

  public NodeRef(final Object vertexId, final Graph graph, V vertex) {
    super(vertexId, graph, vertex);
  }

  @Override
  protected V readFromDisk(final long vertexId) throws IOException {
    return graph.ondiskOverflow.readVertex(vertexId);
  }

  @Override
  public String toString() {
    return StringFactory.vertexString(this);
  }

  // delegate methods start
  @Override
  public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
    return this.get().addEdge(label, inVertex, keyValues);
  }

  @Override
  public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... keyValues) {
    return this.get().property(cardinality, key, value, keyValues);
  }

  @Override
  public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
    return this.get().properties(propertyKeys);
  }

  @Override
  public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
    return this.get().edges(direction, edgeLabels);
  }

  @Override
  public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
    return this.get().vertices(direction, edgeLabels);
  }
  // delegate methods end
}
