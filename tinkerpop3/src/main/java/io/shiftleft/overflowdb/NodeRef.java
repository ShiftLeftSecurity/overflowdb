package io.shiftleft.overflowdb;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;

/**
 * Lightweight (w.r.t. memory usage) reference to for an OdbNode, which is stored in the `node` member.
 * When running low on memory (as detected by {{@link HeapUsageMonitor}}), the {{@link ReferenceManager}} may set
 * that member to `null`, so that the garbage collector can free up some heap, thus avoiding @{@link OutOfMemoryError}.
 * Note that this model only works if nothing else holds references to the {@link OdbNode} - which is therefor strongly
 * discouraged. Instead, the entire application should only ever hold onto {@link NodeRef} instances.
 *
 * When the `node` member is currently null, but is then required (e.g. to lookup a property or an edge), the node will
 * be fetched from the underlying {@link io.shiftleft.overflowdb.storage.OdbStorage}.
 * When OdbGraph is started from an existing storage location, only {@link NodeRef} instances are created - the nodes
 * are lazily on demand as described above.
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
  protected void clear() {
    this.node = null;
  }

  protected byte[] serializeWhenDirty() {
    OdbNode node = this.node;
    if (node != null && node.isDirty()) {
      return graph.storage.serialize(node);
    }
    return null;
  }

  protected void persist(byte[] data) {
    this.graph.storage.persist(id, data);
  }

  public final N get() {
    final N ref = node;
    if (ref != null) {
      /* Node is in memory, just return it */
      return ref;
    } else {
      /* read Node from disk */
      try {
        return getSynchronized();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  /** deserialize node from disk, synchronized to ensure this only happens once in a multi-threaded setup */
  private final synchronized N getSynchronized() throws IOException {
    final N ref = node;
    /* checking again, in case another thread came here first and deserialized the node from disk */
    if (ref != null) {
      return ref;
    } else {
      final N node = readFromDisk(id);
      if (node == null) throw new IllegalStateException("unable to read node from disk; id=" + id);
      this.node = node;
      graph.referenceManager.registerRef(this); // so it can be cleared on low memory
      return node;
    }
  }

  public final Optional<N> getOption() {
    return Optional.ofNullable(node);
  }

  public void setNode(N node) {
    this.node = node;
  }

  private final N readFromDisk(long nodeId) throws IOException {
    return graph.storage.readNode(nodeId);
  }

  @Override
  public Object id() {
    return id;
  }

  @Override
  public OdbGraph graph() {
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
  public Iterator<Edge> outE(String edgeLabel) {
    return this.get().outE(edgeLabel);
  }

  /* specialized version of `edges(Direction, String...)` for efficiency */
  public Iterator<Edge> inE(String edgeLabel) {
    return this.get().inE(edgeLabel);
  }

  @Override
  public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
    return nodes(direction, edgeLabels);
  }

  /* lookup adjacent nodes via direction and labels */
  public Iterator<Vertex> nodes(Direction direction, String... edgeLabels) {
    return this.get().nodes(direction, edgeLabels);
  }

  /* adjacent out nodes (all labels) */
  public Iterator<NodeRef> out() {
    return this.get().out();
  }

  /* adjacent out nodes for a specific label
   * specialized version of `nodes(Direction, String...)` for efficiency */
  public Iterator<NodeRef> out(String edgeLabel) {
    return this.get().out(edgeLabel);
  }

  /* adjacent in nodes (all labels) */
  public Iterator<NodeRef> in() {
    return this.get().in();
  }

  /* adjacent out nodes for a specific label
   * specialized version of `nodes(Direction, String...)` for efficiency */
  public Iterator<NodeRef> in(String edgeLabel) {
    return this.get().in(edgeLabel);
  }

  // delegate methods end

}
