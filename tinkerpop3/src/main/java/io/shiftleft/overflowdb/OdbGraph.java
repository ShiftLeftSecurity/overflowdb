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
import org.apache.commons.collections.iterators.EmptyIterator;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
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
  public final OdbIndexManager indexManager = new OdbIndexManager(this);
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
      initEmptyElementCollections();
    }
    referenceManager = new ReferenceManager(storage);
    heapUsageMonitor = config.isOverflowEnabled() ?
        Optional.of(new HeapUsageMonitor(config.getHeapPercentageThreshold(), referenceManager)) :
        Optional.empty();
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
    indexManager.initializeStoredIndices(storage);
    long elapsedMillis = System.currentTimeMillis() - start;
    logger.info("initialized " + this.toString() + " from existing storage in " + elapsedMillis + "ms");
  }


  ////////////// STRUCTURE API METHODS //////////////////

  public NodeRef addNode(final String label, final Object... keyValues) {
    return addNode(label, currentId.incrementAndGet(), keyValues);
  }

  public NodeRef addNode(final String label, final long id, final Object... keyValues) {
    if (isClosed()) {
      throw new IllegalStateException("cannot add more elements, graph is closed");
    }
    ElementHelper.legalPropertyKeyValueArray(keyValues);
    if (nodes.containsKey(id)) {
      throw Exceptions.vertexWithIdAlreadyExists(id);
    }

    currentId.set(Long.max(id, currentId.get()));
    final NodeRef node = createNode(id, label, keyValues);
    nodes.put(node.id, node);
    storeInByLabelCollection(node);
    return node;
  }

  // TODO: move to tinkerpop-specific OdbGraph wrapper
  @Override
  public Vertex addVertex(final Object... keyValues) {
    final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);
    Optional<Long> suppliedId = ElementHelper.getIdValue(keyValues).map(this::parseLong);
    long id = suppliedId.orElseGet(() -> currentId.incrementAndGet());
    return addNode(label, id, keyValues);
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
      indexManager.storeIndexes(storage);
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
    final Set<NodeRef> nodes = nodesByLabel.get(label);
    if (nodes != null)
      return nodes.iterator();
    else
      return EmptyIterator.INSTANCE;
  }

  public Iterator<NodeRef> nodesByLabel(final String... labels) {
    final MultiIterator<NodeRef> multiIterator = new MultiIterator<>();
    for (String label : labels) {
      addNodesToMultiIterator(multiIterator, label);
    }
    return multiIterator;
  }

  public Iterator<NodeRef> nodesByLabel(final Set<String> labels) {
    final MultiIterator<NodeRef> multiIterator = new MultiIterator<>();
    for (String label : labels) {
      addNodesToMultiIterator(multiIterator, label);
    }
    return multiIterator;
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

  private final void addNodesToMultiIterator(final MultiIterator<NodeRef> multiIterator, final String label) {
    final Set<NodeRef> nodes = nodesByLabel.get(label);
    if (nodes != null) {
      multiIterator.addIterator(nodes.iterator());
    }
  }

  @Override
  public Features features() {
    return features;
  }

  public boolean isClosed() {
    return closed;
  }

  public OdbStorage getStorage() {
    return storage;
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

}
