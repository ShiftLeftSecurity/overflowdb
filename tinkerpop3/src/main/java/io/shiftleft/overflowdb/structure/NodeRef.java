package io.shiftleft.overflowdb.structure;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.io.IOException;
import java.util.Iterator;

/**
 * Wrapper for a node, which may be set to `null` by @ReferenceManager to avoid OutOfMemory errors.
 * When it's cleared, it will be persisted to an on-disk storage.
 */
public abstract class NodeRef<N extends OdbNode> implements Vertex {
  public final long id;
  protected final OdbGraph graph;
  private N node;

  public NodeRef(final OdbGraph graph, N node) {
    this.graph = graph;
    this.node = node;
    this.id = node.ref.id;
  }

  /**
   * used when creating a node without the underlying instance at hand
   * this assumes that it is available on disk
   */
  public NodeRef(final OdbGraph graph, final long id) {
    this.graph = graph;
    this.id = id;
  }

  public boolean isSet() {
    return node != null;
  }

  public boolean isCleared() {
    return node == null;
  }

  /* only called by @ReferenceManager */
  protected void clear() throws IOException {
    OdbNode node = this.node;
    if (node != null) {
      graph.ondiskOverflow.persist(node);
    }
    this.node = null;
  }

  public N get() {
    N ref = node;
    if (ref != null) {
      return ref;
    } else {
      try {
        final N node = readFromDisk(id);
        if (node == null) throw new IllegalStateException("unable to read node from disk; id=" + id);
        this.node = node;
        graph.referenceManager.registerRef(this); // so it can be cleared on low memory
        return node;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public void setNode(N node) {
    this.node = node;
  }

  protected N readFromDisk(long nodeId) throws IOException {
    return graph.ondiskOverflow.readVertex(nodeId);
  }

  @Override
  public Object id() {
    return id;
  }

  @Override
  public Graph graph() {
    return graph;
  }

  // delegate methods start

  @Override
  public void remove() {
    this.get().remove();
  }

  @Override
  public int hashCode() {
    return id().hashCode();
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj instanceof NodeRef) {
      return id().equals(((NodeRef) obj).id());
    } else {
      return false;
    }
  }

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
