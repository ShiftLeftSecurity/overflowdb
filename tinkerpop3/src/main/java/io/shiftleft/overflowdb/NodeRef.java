package io.shiftleft.overflowdb;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;

/**
 * Wrapper for a node, which may be set to `null` by @ReferenceManager and persisted to storage to avoid `OutOfMemory` errors.
 *
 * When starting from an existing storage location, only `NodeRef` instances are created - the underlying nodes
 * are lazily fetched from storage.
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
   */
  public NodeRef(final OdbGraph graph, final long id) {
    this.graph = graph;
    this.id = id;

    // this new NodeRef may refer to an already existing node. if so: assign the underlying node
    final Vertex maybeAlreadyExistent = graph.vertex(id);
    if (maybeAlreadyExistent != null) {
      final Optional<N> nodeOption = ((NodeRef) maybeAlreadyExistent).getOption();
      if (nodeOption.isPresent()) {
        this.node = nodeOption.get();
      }
    }
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
      graph.storage.persist(node);
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

  public Optional<N> getOption() {
    return Optional.ofNullable(node);
  }

  public void setNode(N node) {
    this.node = node;
  }

  protected N readFromDisk(long nodeId) throws IOException {
    return graph.storage.readNode(nodeId);
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

  /* specialized version of `edges(Direction, String...)` for efficiency */
  public Iterator<Edge> edgesOut(String edgeLabel) {
    return this.get().edgesOut(edgeLabel);
  }

  /* specialized version of `edges(Direction, String...)` for efficiency */
  public Iterator<Edge> edgesIn(String edgeLabel) {
    return this.get().edgesIn(edgeLabel);
  }

  @Override
  public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
    return nodes(direction, edgeLabels);
  }

  /* lookup adjacent nodes via direction and labels */
  public Iterator<Vertex> nodes(Direction direction, String... edgeLabels) {
    return this.get().nodes(direction, edgeLabels);
  }

  /* adjacent out nodes for a specific label
   * specialized version of `nodes(Direction, String...)` for efficiency */
  public Iterator<NodeRef> nodesOut(String edgeLabel) {
    return this.get().nodesOut(edgeLabel);
  }

  /* adjacent out nodes for a specific label
   * specialized version of `nodes(Direction, String...)` for efficiency */
  public Iterator<NodeRef> nodesIn(String edgeLabel) {
    return this.get().nodesIn(edgeLabel);
  }

  // delegate methods end

}
