package overflowdb;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Lightweight (w.r.t. memory usage) reference to for an OdbNode, which is stored in the `node` member.
 * When running low on memory (as detected by {{@link HeapUsageMonitor}}), the {{@link ReferenceManager}} may set
 * that member to `null`, so that the garbage collector can free up some heap, thus avoiding @{@link OutOfMemoryError}.
 * Note that this model only works if nothing else holds references to the {@link NodeDb} - which is therefor strongly
 * discouraged. Instead, the entire application should only ever hold onto {@link NodeRef} instances.
 *
 * When the `node` member is currently null, but is then required (e.g. to lookup a property or an edge), the node will
 * be fetched from the underlying {@link overflowdb.storage.OdbStorage}.
 * When OdbGraph is started from an existing storage location, only {@link NodeRef} instances are created - the nodes
 * are lazily on demand as described above.
 */
public abstract class NodeRef<N extends NodeDb> implements Node {
  protected final long id;
  protected final Graph graph;
  private N node;

  public NodeRef(final Graph graph, N node) {
    this.graph = graph;
    this.node = node;
    this.id = node.ref.id;
  }

  /**
   * used when creating a node without the underlying instance at hand
   */
  public NodeRef(final Graph graph, final long id) {
    this.graph = graph;
    this.id = id;

    // this new NodeRef may refer to an already existing node. if so: assign the underlying node
    final Node maybeAlreadyExistent = graph.node(id);
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
    NodeDb node = this.node;
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

  public long id() {
    return id;
  }

  @Override
  public Graph graph() {
    return graph;
  }

  // delegate methods start

  @Override
  public void remove() {
    get().remove();
    clear();
  }

  @Override
  public int hashCode() {
    return Objects.hash(id());
  }

  @Override
  public boolean equals(final Object obj) {
    return (obj instanceof Node) && id() == ((Node) obj).id();
  }

  @Override
  public Edge addEdge(String label, Node inNode, Object... keyValues) {
    return this.get().addEdge(label, inNode, keyValues);
  }

  @Override
  public Edge addEdge(String label, Node inNode, Map<String, Object> keyValues) {
    return this.get().addEdge(label, inNode, keyValues);
  }

  @Override
  public void addEdgeSilent(String label, Node inNode, Object... keyValues) {
    this.get().addEdgeSilent(label, inNode, keyValues);
  }

  @Override
  public void addEdgeSilent(String label, Node inNode, Map<String, Object> keyValues) {
    this.get().addEdgeSilent(label, inNode, keyValues);
  }

  @Override
  public Map<String, Object> propertyMap() {
    return this.get().propertyMap();
  }

  @Override
  public Object property(String propertyKey) {
    return this.get().property(propertyKey);
  }

  @Override
  public <A> A property(PropertyKey<A> key) {
    return get().property(key);
  }

  @Override
  public <A> Optional<A> propertyOption(PropertyKey<A> key) {
    return get().propertyOption(key);
  }

  @Override
  public Optional<Object> propertyOption(String key) {
    return get().propertyOption(key);
  }

  @Override
  public <A> void setProperty(PropertyKey<A> key, A value) {
    get().setProperty(key, value);
  }

  @Override
  public void setProperty(Property<?> property) {
    get().setProperty(property);
  }

  @Override
  public void setProperty(String key, Object value) {
    this.get().setProperty(key, value);
  }

  @Override
  public Set<String> propertyKeys() {
    return this.get().propertyKeys();
  }

  @Override
  public void removeProperty(String key) {
    this.get().removeProperty(key);
  }

  /* adjacent OUT nodes (all labels) */
  @Override
  public Iterator<Node> out() {
    return this.get().out();
  }

  /* adjacent OUT nodes for given labels */
  @Override
  public Iterator<Node> out(String... edgeLabels) {
    return this.get().out(edgeLabels);
  }

  /* adjacent IN nodes (all labels) */
  @Override
  public Iterator<Node> in() {
    return this.get().in();
  }

  /* adjacent IN nodes for given labels */
  @Override
  public Iterator<Node> in(String... edgeLabels) {
    return this.get().in(edgeLabels);
  }

  /* adjacent OUT/IN nodes (all labels) */
  @Override
  public Iterator<Node> both() {
    return this.get().both();
  }

  /* adjacent OUT/IN nodes for given labels */
  @Override
  public Iterator<Node> both(String... edgeLabels) {
    return this.get().both(edgeLabels);
  }

  /* adjacent OUT edges (all labels) */
  @Override
  public Iterator<Edge> outE() {
    return this.get().outE();
  }

  /* adjacent OUT edges for given labels */
  @Override
  public Iterator<Edge> outE(String... edgeLabels) {
    return this.get().outE(edgeLabels);
  }

  /* adjacent IN edges (all labels) */
  @Override
  public Iterator<Edge> inE() {
    return this.get().inE();
  }

  /* adjacent IN edges for given labels */
  @Override
  public Iterator<Edge> inE(String... edgeLabels) {
    return this.get().inE(edgeLabels);
  }

  /* adjacent OUT/IN edges (all labels) */
  @Override
  public Iterator<Edge> bothE() {
    return this.get().bothE();
  }

  /* adjacent OUT/IN edges for given labels */
  @Override
  public Iterator<Edge> bothE(String... edgeLabels) {
    return this.get().bothE(edgeLabels);
  }

  // delegate methods end

  @Override
  public String toString() {
    return getClass().getName() + "[label=" + label() + "; id=" + id + "]";
  }
}
