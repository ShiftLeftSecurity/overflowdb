package overflowdb.tinkerpop;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoVersion;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import overflowdb.Node;
import overflowdb.NodeRef;
import overflowdb.OdbGraph;
import overflowdb.tinkerpop.optimizations.CountStrategy;
import overflowdb.tinkerpop.optimizations.OdbGraphStepStrategy;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Optional;

public final class OdbGraphTp3 implements Graph {
  public final OdbGraph graph;

  public static OdbGraphTp3 wrap(OdbGraph graph) {
    return new OdbGraphTp3(graph);
  }

  private OdbGraphTp3(OdbGraph graph) {
    this.graph = graph;
  }

  static {
    TraversalStrategies.GlobalCache.registerStrategies(OdbGraphTp3.class, TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone().addStrategies(
        OdbGraphStepStrategy.instance(),
        CountStrategy.instance()));
  }

  private final GraphFeatures features = new GraphFeatures();
  protected final GraphVariables variables = new GraphVariables();

  @Override
  public Vertex addVertex(Object... keyValues) {
    final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);
    Optional<Long> suppliedId = ElementHelper.getIdValue(keyValues).map(this::parseLong);

    keyValues = withoutTinkerpopSpecificEntries(keyValues);

    final Node newNode;
    if (suppliedId.isPresent()) {
      newNode = graph.addNode(suppliedId.get(), label, keyValues);
    } else {
      newNode = graph.addNode(label, keyValues);
    }
    return NodeTp3.wrap((NodeRef) newNode);
  }

  private Object[] withoutTinkerpopSpecificEntries(final Object... keyValues) {
    final ArrayList keyValuesWithoutTinkerpopSpecifics = new ArrayList();
    for (int i = 0; i < keyValues.length; i = i + 2) {
      if (!(keyValues[i] instanceof T)) {
        keyValuesWithoutTinkerpopSpecifics.add(keyValues[i]);
        keyValuesWithoutTinkerpopSpecifics.add(keyValues[i + 1]);
      }
    }
    return keyValuesWithoutTinkerpopSpecifics.toArray();
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

  @Override
  public <C extends GraphComputer> C compute(final Class<C> graphComputerClass) {
    throw Exceptions.graphDoesNotSupportProvidedGraphComputer(graphComputerClass);
  }

  @Override
  public GraphComputer compute() {
    throw Exceptions.graphComputerNotSupported();
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
    return StringFactory.graphString(this, "nodes: " + graph.nodeCount());
  }

  /**
   * if the config.graphLocation is set, data in the graph is persisted to that location.
   */
  @Override
  public void close() {
    graph.close();
  }

  @Override
  public Transaction tx() {
    throw Exceptions.transactionsNotSupported();
  }

  @Override
  public Configuration configuration() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Iterator<Vertex> vertices(final Object... idsOrVertices) {
    if (idsOrVertices.length == 0) { //return all nodes as per tinkerpop semantics
      final Iterator<Node> nodeRefIter = graph.nodes();
      return IteratorUtils.map(nodeRefIter, ref -> NodeTp3.wrap((NodeRef) ref));
    } else {
      final long[] ids = new long[idsOrVertices.length];
      int idx = 0;
      for (Object idOrNode : idsOrVertices) {
        ids[idx++] = convertToId(idOrNode);
      }
      final Iterator<Node> nodeRefIter = graph.nodes(ids);
      return IteratorUtils.map(nodeRefIter, ref -> NodeTp3.wrap((NodeRef) ref));
    }
  }

  /** the tinkerpop api allows to pass the actual element instead of the ids :( */
  private Long convertToId(Object idOrNode) {
    if (idOrNode instanceof Long) return (Long) idOrNode;
    else if (idOrNode instanceof Integer) return ((Integer) idOrNode).longValue();
    else if (idOrNode instanceof Vertex) return (Long) ((Vertex) idOrNode).id();
    else throw new IllegalArgumentException("unsupported id type: " + idOrNode.getClass() + " (" + idOrNode + "). Please pass one of [Long, OdbNode, NodeRef].");
  }

  @Override
  public Iterator<Edge> edges(final Object... ids) {
    if (ids.length > 0) throw new IllegalArgumentException("edges only exist virtually, and they don't have ids");
    return IteratorUtils.flatMap(graph.nodes(), node -> NodeTp3.wrap((NodeRef) node).edges(Direction.OUT));
  }

  @Override
  public Features features() {
    return features;
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
