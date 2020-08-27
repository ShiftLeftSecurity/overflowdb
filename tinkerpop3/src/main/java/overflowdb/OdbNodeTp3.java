package overflowdb;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import overflowdb.tinkerpop.OdbNodeProperty;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;

public class OdbNodeTp3 implements Vertex {
  public final OdbNode node;

  public static OdbNodeTp3 wrap(OdbNode node) {
    return new OdbNodeTp3(node);
  }

  private OdbNodeTp3(OdbNode node) {
    this.node = node;
  }

  @Override
  public Graph graph() {
    return OdbGraphTp3.wrap(node.graph2());
  }

  @Override
  public Object id() {
    return node.id2();
  }

  @Override
  public String label() {
    return node.label();
  }

  @Override
  public Set<String> keys() {
    return node.propertyKeys();
  }

  @Override
  public <V> VertexProperty<V> property(String key) {
    Object value = node.property2(key);
    if (value == null)
      return VertexProperty.empty();
    else
      return new OdbNodeProperty<>(this, key, (V) value);
  }

  @Override
  public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
    final Iterator<VertexProperty<V>> properties;
    if (propertyKeys.length == 1) { // treating as special case for performance
      properties = IteratorUtils.of(property(propertyKeys[0]));
    } else if (propertyKeys.length == 0) { // return all properties
      Stream<VertexProperty<V>> vertexPropertyStream = keys().stream().map(this::property);
      properties = vertexPropertyStream.iterator();
    } else {
      Stream<VertexProperty<V>> vertexPropertyStream = Arrays.stream(propertyKeys).map(this::property);
      properties = vertexPropertyStream.iterator();
    }

    return IteratorUtils.filter(properties, p -> p.isPresent());
  }

  @Override
  public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... keyValues) {
    if (cardinality != VertexProperty.Cardinality.single || keyValues.length > 0) {
      throw new UnsupportedOperationException("only Cardinality.single properties are supported for now");
    }
    ElementHelper.validateProperty(key, value);
    node.setProperty(key, value);
    return new OdbNodeProperty<>(this, key, value);
  }

  @Override
  public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
    return OdbEdgeTp3.wrap(
      node.addEdge2(label, (Node) inVertex, keyValues)
    );
  }

  @Override
  public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
    final Iterator<OdbEdge> odbEdgeIterator;
    if (edgeLabels.length == 0) { // tinkerpop convention: follow all available labels
      odbEdgeIterator = allEdges(direction);
    } else {
      odbEdgeIterator = specificEdges(direction, edgeLabels);
    }
    return IteratorUtils.map(odbEdgeIterator, OdbEdgeTp3::wrap);
  }

  private Iterator<OdbEdge> allEdges(Direction direction) {
    switch (direction) {
      case OUT: return node.outE();
      case IN: return node.inE();
      case BOTH: return node.bothE();
      default: throw new UnsupportedOperationException("unknown direction: " + direction);
    }
  }

  private Iterator<OdbEdge> specificEdges(Direction direction, String... labels) {
    switch (direction) {
      case OUT: return node.outE(labels);
      case IN: return node.inE(labels);
      case BOTH: return node.bothE(labels);
      default: throw new UnsupportedOperationException("unknown direction: " + direction);
    }
  }

  @Override
  public Iterator<Vertex> vertices(Direction direction, String... nodeLabels) {
    final Iterator<Node> nodeIterator;
    if (nodeLabels.length == 0) { // tinkerpop convention: follow all available labels
      nodeIterator = allNodes(direction);
    } else {
      nodeIterator = specificNodes(direction, nodeLabels);
    }
    return IteratorUtils.map(nodeIterator, x -> NodeRefTp3.wrap((NodeRef) x));
  }

  private Iterator<Node> allNodes(Direction direction) {
    switch (direction) {
      case OUT: return node.out();
      case IN: return node.in();
      case BOTH: return node.both();
      default: throw new UnsupportedOperationException("unknown direction: " + direction);
    }
  }

  private Iterator<Node> specificNodes(Direction direction, String... labels) {
    switch (direction) {
      case OUT: return node.out(labels);
      case IN: return node.in(labels);
      case BOTH: return node.both(labels);
      default: throw new UnsupportedOperationException("unknown direction: " + direction);
    }
  }

  @Override
  public int hashCode() {
    return node.hashCode();
  }

  @Override
  public boolean equals(final Object obj) {
    return (obj instanceof OdbNodeTp3) && node.equals(obj);
  }

  @Override
  public void remove() {
    node.remove();
  }
}
