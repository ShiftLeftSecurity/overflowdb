package overflowdb;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import overflowdb.tinkerpop.Converters;
import overflowdb.tinkerpop.OdbNodeProperty;
import overflowdb.tinkerpop.OdbProperty;
import overflowdb.util.MultiIterator2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

public class OdbNodeTp3 implements Vertex {
  private final OdbNode node;

  public static OdbNodeTp3 wrap(OdbNode node) {
    return new OdbNodeTp3(node);
  }

  public OdbNodeTp3(OdbNode node) {
    this.node = node;
  }

  protected <V> Iterator<VertexProperty<V>> specificProperties(String key) {
    final Object value = specificProperty2(key);
    if (value != null) return IteratorUtils.of(new OdbNodeProperty(this, key, value));
    else return Collections.emptyIterator();
  }

  @Override
  public Graph graph() {
    return ref.graph;
  }

  @Override
  public Object id() {
    return ref.id;
  }

  @Override
  public String label() {
    return ref.label();
  }

  @Override
  public Set<String> keys() {
    return layoutInformation().propertyKeys();
  }

  @Override
  public <V> VertexProperty<V> property(String key) {
    return specificProperty(key);
  }

  /* You can override this default implementation in concrete specialised instances for performance
   * if you like, since technically the Iterator isn't necessary.
   * This default implementation works fine though. */
  protected <V> VertexProperty<V> specificProperty(String key) {
    Iterator<VertexProperty<V>> iter = specificProperties(key);
    if (iter.hasNext()) {
      return iter.next();
    } else {
      return VertexProperty.empty();
    }
  }

  @Override
  public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
    if (propertyKeys.length == 0) { // return all properties
      return (Iterator) layoutInformation().propertyKeys().stream().flatMap(key ->
          StreamSupport.stream(Spliterators.spliteratorUnknownSize(
              specificProperties(key), Spliterator.ORDERED), false)
      ).iterator();
    } else if (propertyKeys.length == 1) { // treating as special case for performance
      return specificProperties(propertyKeys[0]);
    } else {
      return (Iterator) Arrays.stream(propertyKeys).flatMap(key ->
          StreamSupport.stream(Spliterators.spliteratorUnknownSize(
              specificProperties(key), Spliterator.ORDERED), false)
      ).iterator();
    }
  }

  @Override
  public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... keyValues) {
    ElementHelper.legalPropertyKeyValueArray(keyValues);
    ElementHelper.validateProperty(key, value);
    final VertexProperty<V> vp = updateSpecificProperty(cardinality, key, value);
    ref.graph.indexManager.putIfIndexed(key, value, ref);
    /* marking as dirty *after* we updated - if node gets serialized before we finish, it'll be marked as dirty */
    this.markAsDirty();
    return vp;
  }

  @Override
  public void remove() {
    final List<Edge> edges = new ArrayList<>();
    bothE().forEachRemaining(edges::add);
    for (Edge edge : edges) {
      if (!((OdbEdge) edge).isRemoved()) {
        edge.remove();
      }
    }

    ref.graph.remove(this);

    /* marking as dirty *after* we updated - if node gets serialized before we finish, it'll be marked as dirty */
    this.markAsDirty();
  }

  public <V> Iterator<Property<V>> getEdgeProperties(Direction direction,
                                                     OdbEdge edge,
                                                     int blockOffset,
                                                     String... keys) {
    List<Property<V>> result = new ArrayList<>();

    if (keys.length != 0) {
      for (String key : keys) {
        result.add(getEdgeProperty(direction, edge, blockOffset, key));
      }
    } else {
      for (String propertyKey : layoutInformation().edgePropertyKeys(edge.label())) {
        result.add(getEdgeProperty(direction, edge, blockOffset, propertyKey));
      }
    }

    return result.iterator();
  }

  public Map<String, Object> getEdgePropertyMap(Direction direction, OdbEdge edge, int blockOffset) {
    final Set<String> edgePropertyKeys = layoutInformation().edgePropertyKeys(edge.label());
    final Map<String, Object> results = new HashMap<>(edgePropertyKeys.size());

    for (String propertyKey : edgePropertyKeys) {
      final Object value = getEdgeProperty2(direction, edge, blockOffset, propertyKey);
      if (value != null) results.put(propertyKey, value);
    }

    return results;
  }

  public <V> Property<V> getEdgeProperty(Direction direction,
                                         OdbEdge edge,
                                         int blockOffset,
                                         String key) {
    V value = getEdgeProperty2(direction, edge, blockOffset, key);
    if (value == null) {
      return EmptyProperty.instance();
    }
    return new OdbProperty<>(key, value, edge);
  }

  public <V> void setEdgeProperty(Direction direction,
                                  String edgeLabel,
                                  String key,
                                  V value,
                                  int blockOffset) {
    int propertyPosition = getEdgePropertyIndex(direction, edgeLabel, key, blockOffset);
    if (propertyPosition == -1) {
      throw new RuntimeException("Edge " + edgeLabel + " does not support property `" + key + "`.");
    }
    adjacentNodesWithProperties[propertyPosition] = value;
    /* marking as dirty *after* we updated - if node gets serialized before we finish, it'll be marked as dirty */
    this.markAsDirty();
  }

  @Override
  public Iterator<Edge> edges(org.apache.tinkerpop.gremlin.structure.Direction tinkerDirection, String... edgeLabels) {
    Direction direction = Converters.fromTinker(tinkerDirection);
    final MultiIterator2<Edge> multiIterator = new MultiIterator2<>();
    if (direction == Direction.IN || direction == Direction.BOTH) {
      for (String label : calcInLabels(edgeLabels)) {
        Iterator<OdbEdgeTp3> edgeIterator = createDummyEdgeIterator(Direction.IN, label);
        multiIterator.addIterator(edgeIterator);
      }
    }
    if (direction == Direction.OUT || direction == Direction.BOTH) {
      for (String label : calcOutLabels(edgeLabels)) {
        Iterator<OdbEdgeTp3> edgeIterator = createDummyEdgeIterator(Direction.OUT, label);
        multiIterator.addIterator(edgeIterator);
      }
    }

    return multiIterator;
  }

  @Override
  public Iterator<Vertex> vertices(org.apache.tinkerpop.gremlin.structure.Direction direction, String... edgeLabels) {
    return nodes(Converters.fromTinker(direction), edgeLabels);
  }

  /* lookup adjacent nodes via direction and labels */
  public Iterator<Vertex> nodes(Direction direction, String... edgeLabels) {
    final MultiIterator2<Vertex> multiIterator = new MultiIterator2<>();
    if (direction == Direction.IN || direction == Direction.BOTH) {
      for (String label : calcInLabels(edgeLabels)) {
        multiIterator.addIterator(in(label));
      }
    }
    if (direction == Direction.OUT || direction == Direction.BOTH) {
      for (String label : calcOutLabels(edgeLabels)) {
        multiIterator.addIterator(out(label));
      }
    }

    return multiIterator;
  }

  /* adjacent OUT nodes (all labels) */
  @Override
  public Iterator<Node> out() {
    return createAdjacentNodeIterator(Direction.OUT, ALL_LABELS);
  }

  /* adjacent OUT nodes for given labels */
  @Override
  public Iterator<Node> out(String... edgeLabels) {
    return createAdjacentNodeIterator(Direction.OUT, edgeLabels);
  }

  /* adjacent IN nodes (all labels) */
  @Override
  public Iterator<Node> in() {
    final MultiIterator2<Node> multiIterator = new MultiIterator2<>();
    for (String label : layoutInformation().allowedInEdgeLabels()) {
      multiIterator.addIterator(in(label));
    }
    return multiIterator;
  }

  /* adjacent IN nodes for given labels */
  @Override
  public Iterator<Node> in(String... edgeLabels) {
    return createAdjacentNodeIterator(Direction.IN, edgeLabels);
  }

  /* adjacent OUT/IN nodes (all labels) */
  @Override
  public Iterator<Node> both() {
    final MultiIterator2<Node> multiIterator = new MultiIterator2<>();
    multiIterator.addIterator(out());
    multiIterator.addIterator(in());
    return multiIterator;
  }

  /* adjacent OUT/IN nodes for given labels */
  @Override
  public Iterator<Node> both(String... edgeLabels) {
    final MultiIterator2<Node> multiIterator = new MultiIterator2<>();
    multiIterator.addIterator(out(edgeLabels));
    multiIterator.addIterator(in(edgeLabels));
    return multiIterator;
  }

  /* adjacent OUT edges (all labels) */
  @Override
  public Iterator<OdbEdge> outE() {
    final MultiIterator2<OdbEdge> multiIterator = new MultiIterator2<>();
    for (String label : layoutInformation().allowedOutEdgeLabels()) {
      multiIterator.addIterator(outE(label));
    }
    return multiIterator;
  }

  /* adjacent OUT edges for given labels */
  @Override
  public Iterator<OdbEdge> outE(String... edgeLabels) {
    return createDummyEdgeIterator(Direction.OUT, edgeLabels);
  }

  /* adjacent IN edges (all labels) */
  @Override
  public Iterator<OdbEdge> inE() {
    final MultiIterator2<OdbEdge> multiIterator = new MultiIterator2<>();
    for (String label : layoutInformation().allowedInEdgeLabels()) {
      multiIterator.addIterator(inE(label));
    }
    return multiIterator;
  }

  /* adjacent IN edges for given labels */
  @Override
  public Iterator<OdbEdge> inE(String... edgeLabels) {
    return createDummyEdgeIterator(Direction.IN, edgeLabels);
  }

  /* adjacent OUT/IN edges (all labels) */
  @Override
  public Iterator<OdbEdge> bothE() {
    final MultiIterator2<OdbEdge> multiIterator = new MultiIterator2<>();
    multiIterator.addIterator(outE());
    multiIterator.addIterator(inE());
    return multiIterator;
  }

  /* adjacent OUT/IN edges for given labels */
  @Override
  public Iterator<OdbEdge> bothE(String... edgeLabels) {
    final MultiIterator2<OdbEdge> multiIterator = new MultiIterator2<>();
    multiIterator.addIterator(outE(edgeLabels));
    multiIterator.addIterator(inE(edgeLabels));
    return multiIterator;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id2());
  }

  @Override
  public boolean equals(final Object obj) {
    return (obj instanceof OdbNodeTp3) && id2() == ((OdbNodeTp3) obj).id2();
  }
}
