package overflowdb;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * Lightweight (w.r.t. memory usage) reference to for an NodeDb, which is stored in the `node` member.
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
public abstract class NodeRef<N extends NodeDb> extends Node {
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
    final NodeRef<N> maybeAlreadyExistent = (NodeRef<N>) graph.node(id);
    if (maybeAlreadyExistent != null) {
        this.node = maybeAlreadyExistent.node;
    }
  }

  public boolean isSet() {
    return node != null;
  }

  public boolean isCleared() {
    return node == null;
  }

  /**
   * Only supposed to be called by @NodesWriter
   * We'd prefer this to be package-private, but since NodesWriter is in a different package that's not an option in java.
   * To not pollute the public api (esp. for console users) we made this method static instead.
   * */
  public static void clear(NodeRef ref) {
    ref.node = null;
  }

  protected byte[] serializeWhenDirty() {
    NodeDb node = this.node;
    if (node != null && node.isDirty()) {
      try {
        return graph.nodeSerializer.serialize(node);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
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
      graph.registerNodeRef(this);
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
    byte[] bytes = graph.storage.getSerializedNode(nodeId);
    return (N) graph.nodeDeserializer.deserialize(bytes);
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
  protected void removeImpl() {
    get().removeInternal();
    NodeRef.clear(this);
  }

  @Override
  public int hashCode() {
    /* NodeRef compares by id. We need the hash computation to be fast and allocation-free; but we don't need it
    * very strong. Plain java would use id ^ (id>>>32) ; we do a little bit of mixing.
    * The style (shift-xor 33 and multiply) is similar to murmur3; the multiply constant is randomly chosen odd number.
    * Feel free to change this.
    * */
    long tmp = (id ^ (id >>> 33)) * 0x1ca213a8d7b7d9b1L;
    return ((int) tmp) ^ ((int) (tmp >>> 32));
  }

  @Override
  public boolean equals(final Object obj) {
    return (this == obj) || ( (obj instanceof NodeRef) && id == ((NodeRef) obj).id );
  }

  @Override
  protected Edge addEdgeImpl(String label, Node inNode, Object... keyValues) {
    return this.get().addEdgeInternal(label, inNode, keyValues);
  }

  @Override
  protected Edge addEdgeImpl(String label, Node inNode, Map<String, Object> keyValues) {
    return this.get().addEdgeInternal(label, inNode, keyValues);
  }

  @Override
  protected void addEdgeSilentImpl(String label, Node inNode, Object... keyValues) {
    this.get().addEdgeSilentInternal(label, inNode, keyValues);
  }

  @Override
  protected void addEdgeSilentImpl(String label, Node inNode, Map<String, Object> keyValues) {
    this.get().addEdgeSilentInternal(label, inNode, keyValues);
  }

  @Override
  public Map<String, Object> propertiesMap() {
    return this.get().propertiesMap();
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
  protected <A> void setPropertyImpl(PropertyKey<A> key, A value) {
    get().setPropertyInternal(key, value);
  }

  @Override
  protected void setPropertyImpl(Property<?> property) {
    get().setPropertyInternal(property);
  }

  @Override
  protected void setPropertyImpl(String key, Object value) {
    this.get().setPropertyInternal(key, value);
  }

  @Override
  public Set<String> propertyKeys() {
    return this.get().propertyKeys();
  }

  @Override
  protected void removePropertyImpl(String key) {
    this.get().removePropertyInternal(key);
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

  /*Allows fast initialization from detached node data*/
  @Override
  protected void _initializeFromDetached(DetachedNodeData data, Function<DetachedNodeData, Node> mapper){
    get()._initializeFromDetached(data, mapper);
  }
  // delegate methods end

  @Override
  public String toString() {
    return getClass().getName() + "[label=" + label() + "; id=" + id + "]";
  }
}
