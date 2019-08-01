package io.shiftleft.overflowdb.structure;

import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.THashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.THashSet;
import io.shiftleft.overflowdb.process.traversal.strategy.optimization.CountStrategy;
import io.shiftleft.overflowdb.process.traversal.strategy.optimization.OdbGraphStepStrategy;
import io.shiftleft.overflowdb.storage.NodeDeserializer;
import io.shiftleft.overflowdb.storage.OndiskOverflow;
import io.shiftleft.overflowdb.storage.iterator.MultiIterator2;
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
import java.util.Arrays;
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
  protected Index<Vertex> nodeIndex = null;
  private final OdbConfig config;
  private boolean closed = false;

  protected final Map<String, OdbElementFactory.ForNode> nodeFactoryByLabel;
  protected final Map<String, OdbElementFactory.ForEdge> edgeFactoryByLabel;

  protected final OndiskOverflow ondiskOverflow;
  protected final Optional<HeapUsageMonitor> heapUsageMonitor;
  protected final ReferenceManagerImpl referenceManager;

  public static OdbGraph open(OdbConfig configuration,
                              List<OdbElementFactory.ForNode<?>> nodeFactories,
                              List<OdbElementFactory.ForEdge<?>> edgeFactories) {
    Map<String, OdbElementFactory.ForNode> nodeFactoryByLabel = new HashMap<>();
    Map<String, OdbElementFactory.ForEdge> edgeFactoryByLabel = new HashMap<>();
    nodeFactories.forEach(factory -> nodeFactoryByLabel.put(factory.forLabel(), factory));
    edgeFactories.forEach(factory -> edgeFactoryByLabel.put(factory.forLabel(), factory));
    return new OdbGraph(configuration, nodeFactoryByLabel, edgeFactoryByLabel);
  }

  private OdbGraph(OdbConfig config,
                   Map<String, OdbElementFactory.ForNode> nodeFactoryByLabel,
                   Map<String, OdbElementFactory.ForEdge> edgeFactoryByLabel) {
    this.config = config;
    this.nodeFactoryByLabel = nodeFactoryByLabel;
    this.edgeFactoryByLabel = edgeFactoryByLabel;

    referenceManager = new ReferenceManagerImpl();
    heapUsageMonitor = config.isOverflowEnabled() ?
        Optional.of(new HeapUsageMonitor(config.getHeapPercentageThreshold(), referenceManager)) :
        Optional.empty();

    NodeDeserializer nodeDeserializer = new NodeDeserializer(this, nodeFactoryByLabel);
    if (config.getStorageLocation().isPresent()) {
      ondiskOverflow = OndiskOverflow.createWithSpecificLocation(nodeDeserializer, new File(config.getStorageLocation().get()));
      initElementCollections(ondiskOverflow);
    } else {
      ondiskOverflow = OndiskOverflow.createWithTempFile(nodeDeserializer);
      initEmptyElementCollections();
    }
  }

  private void initEmptyElementCollections() {
    nodes = new TLongObjectHashMap<>();
    nodesByLabel = new THashMap<>(100);
  }

  /**
   * implementation note: must start with vertices, because the edges require the vertexRefs to be already present!
   */
  private void initElementCollections(OndiskOverflow ondiskOverflow) {
    long start = System.currentTimeMillis();
    final Set<Map.Entry<Long, byte[]>> serializedVertices = ondiskOverflow.allVertices();
    logger.info("initializing " + serializedVertices.size() + " nodes from existing storage - this may take some time");
    int importCount = 0;
    long maxId = currentId.get();

    nodes = new TLongObjectHashMap<>(serializedVertices.size());
    nodesByLabel = new THashMap<>(serializedVertices.size());
    final Iterator<Map.Entry<Long, byte[]>> serializedVertexIter = serializedVertices.iterator();
    while (serializedVertexIter.hasNext()) {
      final Map.Entry<Long, byte[]> entry = serializedVertexIter.next();
      try {
        final NodeRef nodeRef = (NodeRef) ondiskOverflow.getVertexDeserializer().get().deserializeRef(entry.getValue());
        nodes.put(nodeRef.id, nodeRef);
        getElementsByLabel(nodesByLabel, nodeRef.label()).add(nodeRef);
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
    getElementsByLabel(nodesByLabel, label).add(node);
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
      throw new IllegalArgumentException(
          "this instance of OverflowDb uses specialized elements, but doesn't have a factory for label " + label
              + ". Mixing specialized and generic elements is not (yet) supported");
    }
    final OdbElementFactory.ForNode factory = nodeFactoryByLabel.get(label);
    final OdbNode underlying = factory.createVertex(idValue, this);
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
    return StringFactory.graphString(this, "vertices: " + nodes.size());
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
    ondiskOverflow.close();
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
    final Iterator<NodeRef> nodeRefIter = nodes.valueCollection().iterator();
    final Iterator<Vertex> vertexIter = IteratorUtils.map(nodeRefIter, ref -> ref); // javac has humour
    if (0 == ids.length) {
      return vertexIter;
    } else {
      final Set idsSet = new HashSet(Arrays.asList(ids));
      return IteratorUtils.filter(vertexIter, ref -> idsSet.contains(ref.id()));
    }
  }

  public int vertexCount() {
    return nodes.size();
  }

  public Iterator<NodeRef> verticesByLabel(final P<String> labelPredicate) {
    return elementsByLabel(nodesByLabel, labelPredicate);
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

  /**
   * retrieve the correct by-label map (and create it if it doesn't yet exist)
   */
  protected <E extends Element> Set<E> getElementsByLabel(final THashMap<String, Set<E>> elementsByLabel, final String label) {
    if (!elementsByLabel.containsKey(label))
      elementsByLabel.put(label, new THashSet<>(100000));
    return elementsByLabel.get(label);
  }

  protected <E extends Element> Iterator<E> elementsByLabel(final THashMap<String, Set<E>> elementsByLabel, final P<String> labelPredicate) {
    final MultiIterator<E> multiIterator = new MultiIterator<>();
    for (String label : elementsByLabel.keySet()) {
      if (labelPredicate.test(label)) {
        multiIterator.addIterator(elementsByLabel.get(label).iterator());
      }
    }
    return multiIterator;
  }

  /**
   * Return OverflowDb feature set.
   * <p/>
   * <b>Reference Implementation Help:</b> Implementers only need to implement features for which there are
   * negative or instance configured features.  By default, all
   * {@link org.apache.tinkerpop.gremlin.structure.Graph.Features} return true.
   */
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
   * Create an index for said element class ({@link Vertex} or {@link Edge}) and said property key.
   * Whenever an element has the specified key mutated, the index is updated.
   * When the index is created, all existing elements are indexed to ensure that they are captured by the index.
   *
   * @param key          the property key to index
   * @param elementClass the element class to index
   * @param <E>          The type of the element class
   */
  public <E extends Element> void createIndex(final String key, final Class<E> elementClass) {
    if (Vertex.class.isAssignableFrom(elementClass)) {
      if (null == this.nodeIndex) this.nodeIndex = new Index<>(this, Vertex.class);
      this.nodeIndex.createKeyIndex(key);
    } else {
      throw new IllegalArgumentException("Class is not indexable: " + elementClass);
    }
  }

  /**
   * Drop the index for the specified element class ({@link Vertex} or {@link Edge}) and key.
   *
   * @param key          the property key to stop indexing
   * @param elementClass the element class of the index to drop
   * @param <E>          The type of the element class
   */
  public <E extends Element> void dropIndex(final String key, final Class<E> elementClass) {
    if (Vertex.class.isAssignableFrom(elementClass)) {
      if (null != this.nodeIndex) this.nodeIndex.dropKeyIndex(key);
    } else {
      throw new IllegalArgumentException("Class is not indexable: " + elementClass);
    }
  }

  /**
   * Return all the keys currently being index for said element class  ({@link Vertex} or {@link Edge}).
   *
   * @param elementClass the element class to get the indexed keys for
   * @param <E>          The type of the element class
   * @return the set of keys currently being indexed
   */
  public <E extends Element> Set<String> getIndexedKeys(final Class<E> elementClass) {
    if (Vertex.class.isAssignableFrom(elementClass)) {
      return null == this.nodeIndex ? Collections.emptySet() : this.nodeIndex.getIndexedKeys();
    } else {
      throw new IllegalArgumentException("Class is not indexable: " + elementClass);
    }
  }
}
