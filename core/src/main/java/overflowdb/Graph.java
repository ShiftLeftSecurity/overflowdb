package overflowdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import overflowdb.storage.NodeDeserializer;
import overflowdb.storage.OdbStorage;
import overflowdb.util.IteratorUtils;
import overflowdb.util.MultiIterator;
import overflowdb.util.NodesList;
import overflowdb.util.PropertyHelper;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

public final class Graph implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(Graph.class);

  protected final AtomicLong currentId = new AtomicLong(-1L);
  protected final NodesList nodes = new NodesList();
  public final IndexManager indexManager = new IndexManager(this);
  private final Config config;
  private boolean closed = false;

  protected final Map<String, NodeFactory> nodeFactoryByLabel;
  protected final Map<String, EdgeFactory> edgeFactoryByLabel;

  protected final OdbStorage storage;
  protected final Optional<HeapUsageMonitor> heapUsageMonitor;
  protected final ReferenceManager referenceManager;

  public static Graph open(Config configuration,
                           List<NodeFactory<?>> nodeFactories,
                           List<EdgeFactory<?>> edgeFactories) {
    Map<String, NodeFactory> nodeFactoryByLabel = new HashMap<>(nodeFactories.size());
    Map<Integer, NodeFactory> nodeFactoryByLabelId = new HashMap<>(nodeFactories.size());
    Map<String, EdgeFactory> edgeFactoryByLabel = new HashMap<>(edgeFactories.size());
    nodeFactories.forEach(factory -> nodeFactoryByLabel.put(factory.forLabel(), factory));
    nodeFactories.forEach(factory -> nodeFactoryByLabelId.put(factory.forLabelId(), factory));
    edgeFactories.forEach(factory -> edgeFactoryByLabel.put(factory.forLabel(), factory));
    return new Graph(configuration, nodeFactoryByLabel, nodeFactoryByLabelId, edgeFactoryByLabel);
  }

  private Graph(Config config,
                Map<String, NodeFactory> nodeFactoryByLabel,
                Map<Integer, NodeFactory> nodeFactoryByLabelId,
                Map<String, EdgeFactory> edgeFactoryByLabel) {
    this.config = config;
    this.nodeFactoryByLabel = nodeFactoryByLabel;
    this.edgeFactoryByLabel = edgeFactoryByLabel;

    NodeDeserializer nodeDeserializer = new NodeDeserializer(
        this, nodeFactoryByLabelId, config.isSerializationStatsEnabled());
    if (config.getStorageLocation().isPresent()) {
      storage = OdbStorage.createWithSpecificLocation(
          nodeDeserializer,
          new File(config.getStorageLocation().get()),
          config.isSerializationStatsEnabled()
      );
      initElementCollections(storage);
    } else {
      storage = OdbStorage.createWithTempFile(nodeDeserializer, config.isSerializationStatsEnabled());
    }
    referenceManager = new ReferenceManager(storage);
    heapUsageMonitor = config.isOverflowEnabled() ?
        Optional.of(new HeapUsageMonitor(config.getHeapPercentageThreshold(), referenceManager)) :
        Optional.empty();
  }

  private void initElementCollections(OdbStorage storage) {
    long start = System.currentTimeMillis();
    final Set<Map.Entry<Long, byte[]>> serializedNodes = storage.allNodes();
    logger.info("initializing " + serializedNodes.size() + " nodes from existing storage - this may take some time");
    int importCount = 0;
    long maxId = currentId.get();

    final Iterator<Map.Entry<Long, byte[]>> serializedVertexIter = serializedNodes.iterator();
    while (serializedVertexIter.hasNext()) {
      final Map.Entry<Long, byte[]> entry = serializedVertexIter.next();
      try {
        final NodeRef nodeRef = storage.getNodeDeserializer().get().deserializeRef(entry.getValue());
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
    logger.debug("initialized " + this.toString() + " from existing storage in " + elapsedMillis + "ms");
  }


  ////////////// STRUCTURE API METHODS //////////////////

  public Node addNode(final String label, final Object... keyValues) {
    return addNode(currentId.incrementAndGet(), label, keyValues);
  }

  public Node addNode(final long id, final String label, final Object... keyValues) {
    if (isClosed()) {
      throw new IllegalStateException("cannot add more elements, graph is closed");
    }
    if (nodes.contains(id)) {
      throw new IllegalArgumentException(String.format("Vertex with id already exists: %s", id));
    }

    currentId.set(Long.max(id, currentId.get()));
    final NodeRef node = createNode(id, label, keyValues);
    nodes.add(node);
    return node;
  }

  private NodeRef createNode(final long idValue, final String label, final Object... keyValues) {
    if (!nodeFactoryByLabel.containsKey(label)) {
      throw new IllegalArgumentException("No NodeFactory for label=" + label + " available.");
    }
    final NodeFactory factory = nodeFactoryByLabel.get(label);
    final NodeDb node = factory.createNode(this, idValue);
    PropertyHelper.attachProperties(node, keyValues);
    this.referenceManager.registerRef(node.ref);

    return node.ref;
  }

  @Override
  public String toString() {
    return String.format("%s [%d nodes]", getClass().getSimpleName(), nodeCount());
  }

  /**
   * if the config.graphLocation is set, data in the graph is persisted to that location.
   */
  @Override
  public void close() {
    this.closed = true;
    try {
      heapUsageMonitor.ifPresent(monitor -> monitor.close());
      if (config.getStorageLocation().isPresent()) {
        /* persist to disk */
        indexManager.storeIndexes(storage);
        referenceManager.clearAllReferences();
      }
    } finally {
      referenceManager.close();
      storage.close();
    }
  }

  public int nodeCount() {
    return nodes.size();
  }

  public int nodeCount(String label) {
    return nodes.cardinality(label);
  }

  /** calculates the number of edges in the graph
   * Note: this is an expensive operation, because edges are stored as part of the nodes
   */
  public int edgeCount() {
    int i = 0;
    final Iterator<Edge> edges = edges();
    while (edges.hasNext()) {
      edges.next();
      i++;
    }
    return i;
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

  public final Node node(long id) {
    return nodes.nodeById(id);
  }

  /** Iterator over nodes with provided ids
   * note: this behaves differently from the tinkerpop api, in that it returns no nodes if no ids are provided */
  public final Iterator<Node> nodes(long... ids) {
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
    final Set<Node> ret = nodes.nodesByLabel(label);
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
      destination.addNode(node.id(), node.label(), PropertyHelper.toKeyValueArray(node.propertyMap()));
    });

    edges().forEachRemaining(edge -> {
      final Node inNode = destination.node(edge.inNode().id());
      final Node outNode = destination.node(edge.outNode().id());
      outNode.addEdge(edge.label(), inNode, PropertyHelper.toKeyValueArray(edge.propertyMap()));
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

}
