package overflowdb;

import gnu.trove.iterator.TObjectIntIterator;
import gnu.trove.map.hash.TObjectIntHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import overflowdb.storage.NodeDeserializer;
import overflowdb.storage.NodeSerializer;
import overflowdb.storage.NodesWriter;
import overflowdb.storage.OdbStorage;
import overflowdb.util.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;

public final class Graph implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(Graph.class);

  protected final AtomicLong currentId = new AtomicLong(-1L);
  final NodesList nodes = new NodesList();
  public final IndexManager indexManager = new IndexManager(this);
  private final Config config;
  private volatile boolean closed = false;

  protected final Map<String, NodeFactory> nodeFactoryByLabel;
  protected final Map<String, EdgeFactory> edgeFactoryByLabel;

  protected final OdbStorage storage;
  public final NodeSerializer nodeSerializer;
  protected final NodeDeserializer nodeDeserializer;
  protected final StringInterner stringInterner;
  protected final Optional<HeapUsageMonitor> heapUsageMonitor;
  protected final boolean overflowEnabled;
  protected final ReferenceManager referenceManager;
  protected final NodesWriter nodesWriter;

  /**
   * @param convertPropertyForPersistence applied to all element property values by @{@link NodeSerializer} prior
   *                                      to persisting nodes/edges. That's useful if your runtime types are not
   *                                      supported by plain java, e.g. because you're using Scala Seq etc.
   */
  public static Graph open(Config configuration,
                           List<NodeFactory<?>> nodeFactories,
                           List<EdgeFactory<?>> edgeFactories,
                           Function<Object, Object> convertPropertyForPersistence) {
    Map<String, NodeFactory> nodeFactoryByLabel = new HashMap<>(nodeFactories.size());
    Map<String, EdgeFactory> edgeFactoryByLabel = new HashMap<>(edgeFactories.size());
    nodeFactories.forEach(factory -> nodeFactoryByLabel.put(factory.forLabel(), factory));
    edgeFactories.forEach(factory -> edgeFactoryByLabel.put(factory.forLabel(), factory));
    return new Graph(configuration, nodeFactoryByLabel, edgeFactoryByLabel, convertPropertyForPersistence);
  }

  public static Graph open(Config configuration,
                           List<NodeFactory<?>> nodeFactories,
                           List<EdgeFactory<?>> edgeFactories) {
    return open(configuration, nodeFactories, edgeFactories, Function.identity());
  }

  private Graph(Config config,
                Map<String, NodeFactory> nodeFactoryByLabel,
                Map<String, EdgeFactory> edgeFactoryByLabel,
                Function<Object, Object> convertPropertyForPersistence) {
    this.config = config;
    this.nodeFactoryByLabel = nodeFactoryByLabel;
    this.edgeFactoryByLabel = edgeFactoryByLabel;
    this.stringInterner = new StringInterner();

    this.storage = config.getStorageLocation().isPresent()
        ? OdbStorage.createWithSpecificLocation(config.getStorageLocation().get().toFile(), stringInterner)
        : OdbStorage.createWithTempFile(stringInterner);
    this.nodeDeserializer = new NodeDeserializer(this, nodeFactoryByLabel, config.isSerializationStatsEnabled(), storage);
    this.nodeSerializer = new NodeSerializer(config.isSerializationStatsEnabled(), storage, convertPropertyForPersistence);
    this.nodesWriter = new NodesWriter(nodeSerializer, storage);
    config.getStorageLocation().ifPresent(l -> initElementCollections(storage));

    this.overflowEnabled = config.isOverflowEnabled();
    if (this.overflowEnabled) {
      if (config.getExecutorService().isPresent()) {
        this.referenceManager = new ReferenceManager(storage, nodesWriter, config.getExecutorService().get());
      } else {
        this.referenceManager = new ReferenceManager(storage, nodesWriter);
      }
      this.heapUsageMonitor = Optional.of(new HeapUsageMonitor(config.getHeapPercentageThreshold(), this.referenceManager));
    } else {
      this.referenceManager = null; // not using Optional only due to performance reasons - it's invoked *a lot*
      this.heapUsageMonitor = Optional.empty();
    }
  }

  private void initElementCollections(OdbStorage storage) {
    long start = System.currentTimeMillis();
    final Set<Map.Entry<Long, byte[]>> serializedNodes = storage.allNodes();
    final int serializedNodesCount = serializedNodes.size();
    if (serializedNodesCount > 0) {
      logger.info(String.format("initializing %d nodes from existing storage", serializedNodesCount));
    }
    int importCount = 0;
    long maxId = currentId.get();

    final Iterator<Map.Entry<Long, byte[]>> serializedVertexIter = serializedNodes.iterator();
    while (serializedVertexIter.hasNext()) {
      final Map.Entry<Long, byte[]> entry = serializedVertexIter.next();
      try {
        final NodeRef nodeRef = nodeDeserializer.deserializeRef(entry.getValue());
        nodes.add(nodeRef);
        importCount++;
        if (importCount % 131072 == 0) { // some random magic number that allows for quick division
          logger.debug("imported " + importCount + " elements - still running...");
        }
        if (nodeRef.id > maxId) maxId = nodeRef.id;
      } catch (IOException e) {
        throw new RuntimeException("error while initializing vertex from storage: id=" + entry.getKey(), e);
      }
    }

    currentId.set(maxId + 1);
    indexManager.initializeStoredIndices(storage);
    long elapsedMillis = System.currentTimeMillis() - start;
    logger.debug(String.format("initialized %s from existing storage in %sms", this, elapsedMillis));
  }


  ////////////// STRUCTURE API METHODS //////////////////

  /**
   * Add a node with given label and properties
   * Will automatically assign an ID - this is the safest option to avoid ID clashes.
   */
  public Node addNode(final String label, final Object... keyValues) {
    return addNodeInternal(currentId.incrementAndGet(), label, keyValues);
  }

  /**
   * Add a node with given id, label and properties.
   * Throws an {@link IllegalArgumentException} if a node with the given ID already exists
   */
  public Node addNode(final long id, final String label, final Object... keyValues) {
    if (nodes.contains(id)) {
      throw new IllegalArgumentException(String.format("Node with id already exists: %s", id));
    }

    long currentIdBefore = currentId.get();
    long currentIdAfter = Long.max(id, currentId.get());
    if (!currentId.compareAndSet(currentIdBefore, currentIdAfter)) {
      // concurrent thread must have changed `currentId` - try again
      return addNode(id, label, keyValues);
    }
    return addNodeInternal(id, label, keyValues);
  }

  private Node addNodeInternal(long id, String label, Object... keyValues) {
    if (isClosed()) {
      throw new AssertionError("graph is closed - no more mutation allowed");
    }
    final NodeRef node = createNode(id, label, keyValues);
    nodes.add(node);
    return node;
  }

  public DetachedNodeData createDetached(String label){
    if (!nodeFactoryByLabel.containsKey(label)) {
      throw new IllegalArgumentException("No NodeFactory for label=" + label + " available.");
    }
    final NodeFactory factory = nodeFactoryByLabel.get(label);
    return factory.createDetached();
  }

  private NodeRef createNode(final long idValue, final String label, final Object... keyValues) {
    if (isClosed()) {
      throw new AssertionError("graph is closed - no more mutation allowed");
    }
    if (!nodeFactoryByLabel.containsKey(label)) {
      throw new IllegalArgumentException("No NodeFactory for label=" + label + " available.");
    }
    final NodeFactory factory = nodeFactoryByLabel.get(label);
    final NodeDb node = factory.createNode(this, idValue, null);
    PropertyHelper.attachProperties(node, keyValues);
    registerNodeRef(node.ref);

    return node.ref;
  }

  /**
   * When we're running low on heap memory we'll serialize some elements to disk. To ensure we're not creating new ones
   * faster than old ones are serialized away, we're applying some backpressure to those newly created ones.
   */
  public void applyBackpressureMaybe() {
    if (referenceManager != null) {
      referenceManager.applyBackpressureMaybe();
    }
  }

  /* Register NodeRef at ReferenceManager, so it can be cleared on low memory */
  public void registerNodeRef(NodeRef ref) {
    if (referenceManager != null && !isClosed()) {
      referenceManager.registerRef(ref);
    }
  }

  @Override
  public String toString() {
    return String.format("%s [%d nodes]", getClass().getSimpleName(), nodeCount());
  }

  /**
   * If the config.graphLocation is set, data in the graph is persisted to that location.
   *
   * If called from multiple threads concurrently, only one starts the shutdown process, but the other one will
   * still be blocked. This is intentional: we also want the second caller to block until `close` is completed, and not
   * falsely assume that it has finished, only because it exits straight away.
   */
  @Override
  public synchronized void close() {
    if (isClosed()) {
      logger.info("graph is already closed");
    } else {
      this.closed = true;
      shutdownNow();
      stringInterner.clear();
    }
  }

  private void shutdownNow() {
    logger.info("shutdown: start");
    try {
      heapUsageMonitor.ifPresent(monitor -> monitor.close());
      if (config.getStorageLocation().isPresent()) {

        /* persist to disk: if overflow is enabled, ReferenceManager takes care of that
         * otherwise: persist all nodes here */
        indexManager.storeIndexes(storage);
        if (referenceManager != null) {
          referenceManager.clearAllReferences();
        } else {
          nodes.persistAll(nodesWriter);
        }
      }
    } finally {
      if (referenceManager != null) {
        referenceManager.close();
      }
      storage.close();
    }
    logger.info("shutdown finished");
  }

  /** overall number of nodes */
  public int nodeCount() {
    return nodes.size();
  }

  /** number of nodes for given label */
  public int nodeCount(String label) {
    return nodes.cardinality(label);
  }

  /** number of nodes grouped by label */
  public Map<String, Integer> nodeCountByLabel() {
    Set<String> nodeLabels = nodes.nodeLabels();
    HashMap counts = new HashMap<String, Integer>(nodeLabels.size());
    for (String label : nodeLabels) {
      counts.put(label, nodes.nodesByLabel(label).size());
    }
    return counts;
  }

  /** calculates the number of edges in the graph
   * Note: this is an expensive operation, because edges are stored as part of the nodes
   */
  public int edgeCount() {
    int edgeCount = 0;
    final Iterator<Node> nodes = nodes();
    while (nodes.hasNext()) {
      NodeDb node = getNodeDb(nodes.next());
      edgeCount += node.outEdgeCount();
    }
    return edgeCount;
  }

  /** number of edges grouped by label */
  public Map<String, Integer> edgeCountByLabel() {
    TObjectIntHashMap<String> counts = new TObjectIntHashMap<>();
    edges().forEachRemaining(edge ->
      counts.adjustOrPutValue(edge.label(), 1, 1)
    );

    Map<String, Integer> ret = new HashMap<>(counts.size());
    TObjectIntIterator<String> iterator = counts.iterator();
    while (iterator.hasNext()) {
      iterator.advance();
      ret.put(iterator.key(), iterator.value());
    }
    return ret;
  }

  /** Iterator over all edges - alias for `edges` */
  public Iterator<Edge> E() {
    return edges();
  }

  /** Iterator over all edges */
  public Iterator<Edge> edges() {
    return IteratorUtils.flatMap(nodes(), node -> node.outE());
  }

  /** Iterator over edges with given label */
  public Iterator<Edge> edges(String label) {
    return IteratorUtils.flatMap(nodes(), node -> node.outE(label));
  }

  /** Iterator over all nodes - alias for `nodes` */
  public Iterator<Node> V() {
    return nodes();
  }

  /** Iterator over all nodes */
  public final Iterator<Node> nodes() {
    return nodes.iterator();
  }

  /** Iterator over nodes with provided ids - alias for `nodes(ids...)`
   * note: does not return any nodes if no ids are provided */
  public Iterator<Node> V(long... ids) {
    return nodes(ids);
  }

  /** return node with given `id`, or `null` if there is no such node */
  public Node node(long id) {
    return nodes.nodeById(id);
  }

  /** Iterator over nodes with provided ids
   * empty, if no ids are provided */
  public Iterator<Node> nodes(long... ids) {
    if (ids.length == 0) {
      return Collections.emptyIterator();
    } else if (ids.length == 1) {
      // optimization for common case where only one id is requested
      return IteratorUtils.from(node(ids[0]));
    } else {
      return IteratorUtils.map(
          Arrays.stream(ids).iterator(),
          this::node);
    }
  }

  public Iterator<Node> nodes(final String label) {
    return nodes.nodesByLabel(label).iterator();
  }

  public Iterator<Node> nodes(final String... labels) {
    final MultiIterator<Node> multiIterator = new MultiIterator<>();
    for (String label : labels) {
      addNodesToMultiIterator(multiIterator, label);
    }
    return multiIterator;
  }

  public Iterator<Node> nodes(final Set<String> labels) {
    final MultiIterator<Node> multiIterator = new MultiIterator<>();
    for (String label : labels) {
      addNodesToMultiIterator(multiIterator, label);
    }
    return multiIterator;
  }

  public Iterator<Node> nodes(final Predicate<String> labelPredicate) {
    final MultiIterator<Node> multiIterator = new MultiIterator<>();
    for (String label : nodes.nodeLabels()) {
      if (labelPredicate.test(label)) {
        addNodesToMultiIterator(multiIterator, label);
      }
    }
    return multiIterator;
  }

  private final void addNodesToMultiIterator(final MultiIterator<Node> multiIterator, final String label) {
    final Collection<Node> ret = nodes.nodesByLabel(label);
    if (ret != null) {
      multiIterator.addIterator(ret.iterator());
    }
  }

  public boolean isClosed() {
    return closed;
  }

  public OdbStorage getStorage() {
    return storage;
  }

  /** Copies all nodes/edges into the given empty graph, preserving their ids and properties. */
  public void copyTo(Graph destination) {
    if (destination.nodeCount() > 0) throw new AssertionError("destination graph must be empty, but isn't");
    nodes().forEachRemaining(node -> {
      destination.addNode(node.id(), node.label(), PropertyHelper.toKeyValueArray(node.propertiesMap()));
    });
    nodes().forEachRemaining( node -> {
      NodeDb mapped =  ((NodeRef<NodeDb>) destination.node(node.id())).get();

      node.outE().forEachRemaining(edge -> {
                NodeRef<?> other = (NodeRef<?>) destination.node(edge.inNode().id());
                mapped.storeAdjacentNode(Direction.OUT, edge.label(), other, PropertyHelper.toKeyValueArray(edge.propertiesMap()));
      });
      node.inE().forEachRemaining(edge -> {
        NodeRef<?> other = (NodeRef<?>) destination.node(edge.outNode().id());
        mapped.storeAdjacentNode(Direction.IN, edge.label(), other, PropertyHelper.toKeyValueArray(edge.propertiesMap()));
      });

    });
  }

  public void remove(Node node) {
    final NodeRef nodeRef = getNodeRef(node);
    nodes.remove(nodeRef);
    indexManager.removeElement(nodeRef);
    storage.removeNode(node.id());
  }

  private NodeRef getNodeRef(Node node) {
    if (node instanceof NodeRef)
      return (NodeRef) node;
    else
      return ((NodeDb) node).ref;
  }

  private NodeDb getNodeDb(Node node) {
    if (node instanceof NodeDb)
      return (NodeDb) node;
    else
      return ((NodeRef) node).get();
  }

  public void persistLibraryVersion(String name, String version) {
    storage.persistLibraryVersion(name, version);
  }

  public ArrayList<Map<String, String>> getAllLibraryVersions() {
    return storage.getAllLibraryVersions();
  }

  public StringInterner getStringInterner() {
    return this.stringInterner;
  }
}
