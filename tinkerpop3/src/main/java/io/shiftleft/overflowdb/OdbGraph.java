package io.shiftleft.overflowdb;

import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.THashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.THashSet;
import io.shiftleft.overflowdb.storage.NodeDeserializer;
import io.shiftleft.overflowdb.storage.OdbStorage;
import io.shiftleft.overflowdb.tp3.GraphVariables;
import io.shiftleft.overflowdb.tp3.TinkerIoRegistryV1d0;
import io.shiftleft.overflowdb.tp3.TinkerIoRegistryV2d0;
import io.shiftleft.overflowdb.tp3.TinkerIoRegistryV3d0;
import io.shiftleft.overflowdb.tp3.optimizations.CountStrategy;
import io.shiftleft.overflowdb.tp3.optimizations.OdbGraphStepStrategy;
import io.shiftleft.overflowdb.util.MultiIterator2;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoVersion;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.tinkerpop.gremlin.util.iterator.MultiIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public final class OdbGraph implements Graph {
  private final Logger logger = LoggerFactory.getLogger(getClass());

  static {
    TraversalStrategies.GlobalCache.registerStrategies(OdbGraph.class, TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone().addStrategies(
        OdbGraphStepStrategy.instance(),
        CountStrategy.instance()));
  }

  private final GraphFeatures features = new GraphFeatures();
  protected final AtomicLong currentId = new AtomicLong(-1L);
  protected TLongObjectMap<NodeRef> nodes;
  protected THashMap<String, Set<NodeRef>> nodesByLabel;
  protected final GraphVariables variables = new GraphVariables();
  protected NodePropertiesIndex<Vertex> nodeIndex = null;
  private final OdbConfig config;
  private boolean closed = false;

  protected final Map<String, NodeFactory> nodeFactoryByLabel;
  protected final Map<String, EdgeFactory> edgeFactoryByLabel;

  protected final OdbStorage storage;
  protected final Optional<HeapUsageMonitor> heapUsageMonitor;
  protected final ReferenceManager referenceManager;

  public static OdbGraph open(OdbConfig configuration,
                              List<NodeFactory<?>> nodeFactories,
                              List<EdgeFactory<?>> edgeFactories) {
    Map<String, NodeFactory> nodeFactoryByLabel = new HashMap<>(nodeFactories.size());
    Map<Integer, NodeFactory> nodeFactoryByLabelId = new HashMap<>(nodeFactories.size());
    Map<String, EdgeFactory> edgeFactoryByLabel = new HashMap<>(edgeFactories.size());
    nodeFactories.forEach(factory -> nodeFactoryByLabel.put(factory.forLabel(), factory));
    nodeFactories.forEach(factory -> nodeFactoryByLabelId.put(factory.forLabelId(), factory));
    edgeFactories.forEach(factory -> edgeFactoryByLabel.put(factory.forLabel(), factory));
    return new OdbGraph(configuration, nodeFactoryByLabel, nodeFactoryByLabelId, edgeFactoryByLabel);
  }

  private OdbGraph(OdbConfig config,
                   Map<String, NodeFactory> nodeFactoryByLabel,
                   Map<Integer, NodeFactory> nodeFactoryByLabelId,
                   Map<String, EdgeFactory> edgeFactoryByLabel) {
    this.config = config;
    this.nodeFactoryByLabel = nodeFactoryByLabel;
    this.edgeFactoryByLabel = edgeFactoryByLabel;

    referenceManager = new ReferenceManager();
    heapUsageMonitor = config.isOverflowEnabled() ?
        Optional.of(new HeapUsageMonitor(config.getHeapPercentageThreshold(), referenceManager)) :
        Optional.empty();

    NodeDeserializer nodeDeserializer = new NodeDeserializer(this, nodeFactoryByLabelId);
    if (config.getStorageLocation().isPresent()) {
      storage = OdbStorage.createWithSpecificLocation(nodeDeserializer, new File(config.getStorageLocation().get()));
      initElementCollections(storage);
    } else {
      storage = OdbStorage.createWithTempFile(nodeDeserializer);
      initEmptyElementCollections();
    }
  }

  private void initEmptyElementCollections() {
    nodes = new TLongObjectHashMap<>();
    nodesByLabel = new THashMap<>(100);
  }

  private void initElementCollections(OdbStorage storage) {
    long start = System.currentTimeMillis();
    final Set<Map.Entry<Long, byte[]>> serializedNodes = storage.allNodes();
    logger.info("initializing " + serializedNodes.size() + " nodes from existing storage - this may take some time");
    int importCount = 0;
    long maxId = currentId.get();

    nodes = new TLongObjectHashMap<>(serializedNodes.size());
    nodesByLabel = new THashMap<>(serializedNodes.size());
    final Iterator<Map.Entry<Long, byte[]>> serializedVertexIter = serializedNodes.iterator();
    while (serializedVertexIter.hasNext()) {
      final Map.Entry<Long, byte[]> entry = serializedVertexIter.next();
      try {
        final NodeRef nodeRef = storage.getNodeDeserializer().get().deserializeRef(entry.getValue());
        nodes.put(nodeRef.id, nodeRef);
        storeInByLabelCollection(nodeRef);
        importCount++;
        if (importCount % 131072 == 0) {
          logger.debug("imported " + importCount + " elements - still running...");
        }
        if (nodeRef.id > maxId) maxId = nodeRef.id;
      } catch (IOException e) {
        throw new RuntimeException("error while initializing vertex from storage: id=" + entry.getKey(), e);
      }
    }

    currentId.set(maxId + 1);
    long elapsedMillis = System.currentTimeMillis() - start;
    logger.info("initialized " + this.toString() + " from existing storage in " + elapsedMillis + "ms");
  }

  ////////////// STRUCTURE API METHODS //////////////////
  @Override
  public Vertex addVertex(final Object... keyValues) {
    if (isClosed()) {
      throw new IllegalStateException("cannot add more elements, graph is closed");
    }
    ElementHelper.legalPropertyKeyValueArray(keyValues);
    final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);

    final long idValue = determineNewNodeId(keyValues);
    currentId.set(Long.max(idValue, currentId.get()));

    final NodeRef node = createNode(idValue, label, keyValues);
    nodes.put(node.id, node);
    storeInByLabelCollection(node);
    return node;
  }

  private long determineNewNodeId(final Object... keyValues) {
    Optional idValueMaybe = ElementHelper.getIdValue(keyValues);
    if (idValueMaybe.isPresent()) {
      final long idValue = parseLong(idValueMaybe.get());
      if (nodes.containsKey(idValue)) {
        throw Exceptions.vertexWithIdAlreadyExists(idValue);
      }
      return idValue;
    } else {
      return currentId.incrementAndGet();
    }
  }

  private long parseLong(Object id) {
    if (id instanceof Long)
      return (long) id;
    else if (id instanceof Number)
      return ((Number) id).longValue();
    else if (id instanceof String)
      return Long.parseLong((String) id);
    else
      throw new IllegalArgumentException(String.format("Expected an id that is convertible to Long but received %s", id.getClass()));
  }

  private NodeRef createNode(final long idValue, final String label, final Object... keyValues) {
    final NodeRef node;
    if (!nodeFactoryByLabel.containsKey(label)) {
      throw new IllegalArgumentException("No NodeFactory for label=" + label + " available.");
    }
    final NodeFactory factory = nodeFactoryByLabel.get(label);
    final OdbNode underlying = factory.createNode(this, idValue);
    this.referenceManager.registerRef(underlying.ref);
    node = underlying.ref;
    ElementHelper.attachProperties(node, VertexProperty.Cardinality.list, keyValues);
    return node;
  }

  @Override
  public <C extends GraphComputer> C compute(final Class<C> graphComputerClass) {
    throw Graph.Exceptions.graphDoesNotSupportProvidedGraphComputer(graphComputerClass);
  }

  @Override
  public GraphComputer compute() {
    throw Graph.Exceptions.graphComputerNotSupported();
  }

  @Override
  public Variables variables() {
    return this.variables;
  }

  @Override
  public <I extends Io> I io(final Io.Builder<I> builder) {
    if (builder.requiresVersion(GryoVersion.V1_0) || builder.requiresVersion(GraphSONVersion.V1_0))
      return (I) builder.graph(this).onMapper(mapper -> mapper.addRegistry(TinkerIoRegistryV1d0.instance())).create();
    else if (builder.requiresVersion(GraphSONVersion.V2_0))   // there is no gryo v2
      return (I) builder.graph(this).onMapper(mapper -> mapper.addRegistry(TinkerIoRegistryV2d0.instance())).create();
    else
      return (I) builder.graph(this).onMapper(mapper -> mapper.addRegistry(TinkerIoRegistryV3d0.instance())).create();
  }

  @Override
  public String toString() {
    return StringFactory.graphString(this, "nodes: " + nodes.size());
  }

  /**
   * if the config.graphLocation is set, data in the graph is persisted to that location.
   */
  @Override
  public void close() {
    this.closed = true;
    heapUsageMonitor.ifPresent(monitor -> monitor.close());
    if (config.getStorageLocation().isPresent()) {
      /* persist to disk */
      referenceManager.clearAllReferences();
    }
    referenceManager.close();
    storage.close();
  }

  @Override
  public Transaction tx() {
    throw Exceptions.transactionsNotSupported();
  }

  @Override
  public Configuration configuration() {
    throw new NotImplementedException("");
  }

  public Vertex vertex(final Long id) {
    return nodes.get(id);
  }

  @Override
  public Iterator<Vertex> vertices(final Object... ids) {
    if (ids.length == 0) { //return all nodes - that's how the tinkerpop api rolls.
      final Iterator<NodeRef> nodeRefIter = nodes.valueCollection().iterator();
      return IteratorUtils.map(nodeRefIter, ref -> ref); // javac has humour
    } else if (ids.length == 1) {
      // optimization for common case where only one id is requested
      final Long id = convertToId(ids[0]);
      return IteratorUtils.of(nodes.get(id));
    } else {
      final Set<Long> idsSet = new HashSet<>(ids.length);
      for (Object idOrNode : ids) {
        idsSet.add(convertToId(idOrNode));
      }
      return IteratorUtils.map(idsSet.iterator(), id -> nodes.get(id));
    }
  }

  /** the tinkerpop api allows to pass the actual element instead of the ids :( */
  private Long convertToId(Object idOrNode) {
    if (idOrNode instanceof Long) return (Long) idOrNode;
    else if (idOrNode instanceof Integer) return ((Integer) idOrNode).longValue();
    else if (idOrNode instanceof Vertex) return (Long) ((Vertex) idOrNode).id();
    else throw new IllegalArgumentException("unsupported id type: " + idOrNode.getClass() + " (" + idOrNode + "). Please pass one of [Long, OdbNode, NodeRef].");
  }

  public int nodeCount() {
    return nodes.size();
  }

  @Override
  public Iterator<Edge> edges(final Object... ids) {
    if (ids.length > 0) throw new IllegalArgumentException("edges only exist virtually, and they don't have ids");
    MultiIterator2 multiIterator = new MultiIterator2();
    nodes.forEachValue(vertex -> {
      multiIterator.addIterator(vertex.edges(Direction.OUT));
      return true;
    });
    return multiIterator;
  }

  private void storeInByLabelCollection(NodeRef nodeRef) {
    final String label = nodeRef.label();
    if (!nodesByLabel.containsKey(label))
      nodesByLabel.put(label, new THashSet<>(1));

    nodesByLabel.get(label).add(nodeRef);
  }

  public Iterator<NodeRef> nodesByLabel(final String label) {
    return nodesByLabel.get(label).iterator();
  }

  public Iterator<NodeRef> nodesByLabel(final P<String> labelPredicate) {
    final MultiIterator<NodeRef> multiIterator = new MultiIterator<>();
    for (String label : nodesByLabel.keySet()) {
      if (labelPredicate.test(label)) {
        multiIterator.addIterator(nodesByLabel.get(label).iterator());
      }
    }
    return multiIterator;
  }

  @Override
  public Features features() {
    return features;
  }

  public boolean isClosed() {
    return closed;
  }

  public class GraphFeatures implements Features {
    private final OdbGraphFeatures graphFeatures = new OdbGraphFeatures();
    private final OdbEdgeFeatures edgeFeatures = new OdbEdgeFeatures();
    private final OdbVertexFeatures vertexFeatures = new OdbVertexFeatures();

    private GraphFeatures() {
    }

    @Override
    public GraphFeatures graph() {
      return graphFeatures;
    }

    @Override
    public EdgeFeatures edge() {
      return edgeFeatures;
    }

    @Override
    public VertexFeatures vertex() {
      return vertexFeatures;
    }

    @Override
    public String toString() {
      return StringFactory.featureString(this);
    }
  }

  public class OdbVertexFeatures implements Features.VertexFeatures {

    private final OdbVertexPropertyFeatures vertexPropertyFeatures = new OdbVertexPropertyFeatures();

    private OdbVertexFeatures() {
    }

    @Override
    public Features.VertexPropertyFeatures properties() {
      return vertexPropertyFeatures;
    }

    @Override
    public boolean supportsCustomIds() {
      return true;
    }

    @Override
    public boolean willAllowId(final Object id) {
      return id instanceof Number || id instanceof String;
    }

    @Override
    public VertexProperty.Cardinality getCardinality(final String key) {
      return VertexProperty.Cardinality.single;
    }
  }

  public class OdbEdgeFeatures implements Features.EdgeFeatures {

    private OdbEdgeFeatures() {
    }

    @Override
    public boolean supportsCustomIds() {
      return false;
    }

    @Override
    public boolean willAllowId(final Object id) {
      return false;
    }
  }

  public class OdbGraphFeatures implements Features.GraphFeatures {

    private OdbGraphFeatures() {
    }

    @Override
    public boolean supportsConcurrentAccess() {
      return false;
    }

    @Override
    public boolean supportsTransactions() {
      return false;
    }

    @Override
    public boolean supportsThreadedTransactions() {
      return false;
    }

  }

  public class OdbVertexPropertyFeatures implements Features.VertexPropertyFeatures {

    private OdbVertexPropertyFeatures() {
    }

    @Override
    public boolean supportsCustomIds() {
      return false;
    }

    @Override
    public boolean willAllowId(final Object id) {
      return false;
    }
  }

  ///////////// GRAPH SPECIFIC INDEXING METHODS ///////////////

  /**
   * Create an index for specified node property.
   * Whenever an element has the specified key mutated, the index is updated.
   * When the index is created, all existing elements are indexed to ensure that they are captured by the index.
   */
  public void createNodePropertyIndex(final String key) {
    if (null == this.nodeIndex) this.nodeIndex = new NodePropertiesIndex(this);
    this.nodeIndex.createKeyIndex(key);
  }
//
//  /**
//   * Drop the index for the specified element class ({@link Vertex} or {@link Edge}) and key.
//   *
//   * @param key          the property key to stop indexing
//   * @param elementClass the element class of the index to drop
//   * @param <E>          The type of the element class
//   */
//  public <E extends Element> void dropIndex(final String key, final Class<E> elementClass) {
//    if (Vertex.class.isAssignableFrom(elementClass)) {
//      if (null != this.nodeIndex) this.nodeIndex.dropKeyIndex(key);
//    } else {
//      throw new IllegalArgumentException("Class is not indexable: " + elementClass);
//    }
//  }
//
  /**
   * Return all the keys currently being indexed for nodes.
   */
  public <E extends Element> Set<String> getIndexedNodeProperties() {
    return null == this.nodeIndex ? Collections.emptySet() : this.nodeIndex.getIndexedKeys();
  }
}
