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

public class NodeRefTp3<N extends OdbNode> implements Vertex {
  public final NodeRef<N> nodeRef;

  public static <N extends OdbNode> NodeRefTp3 wrap(NodeRef<N> nodeRef) {
    return new NodeRefTp3<>(nodeRef);
  }

  private NodeRefTp3(NodeRef<N> nodeRef) {
    this.nodeRef = nodeRef;
  }

  @Override
  public Object id() {
    return nodeRef.id2();
  }

  @Override
  public String label() {
    return nodeRef.label();
  }

  @Override
  public Graph graph() {
    return OdbGraphTp3.wrap(nodeRef.graph2());
  }

  @Override
  public Set<String> keys() {
    return nodeRef.propertyKeys();
  }

  @Override
  public int hashCode() {
    return nodeRef.hashCode();
  }

  @Override
  public void remove() {
    nodeRef.remove();
  }

  @Override
  public boolean equals(final Object obj) {
    return nodeRef.equals(obj);
  }

  @Override
  public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
    NodeRef inNode = ((NodeRefTp3) inVertex).nodeRef;
    final OdbEdge odbEdge = nodeRef.addEdge2(label, inNode, keyValues);
    return OdbEdgeTp3.wrap(odbEdge);
  }

  @Override
  public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... keyValues) {
    if (cardinality != VertexProperty.Cardinality.single || keyValues.length > 0) {
      throw new UnsupportedOperationException("only Cardinality.single properties are supported for now");
    }
    ElementHelper.validateProperty(key, value);
    nodeRef.setProperty(key, value);
    return new OdbNodeProperty<>(this, key, value);
  }

  @Override
  public <V> VertexProperty<V> property(String key) {
    Object value = nodeRef.property2(key);
    if (value == null)
      return VertexProperty.empty();
    else
      return new OdbNodeProperty<>(this, key, (V) value);
  }

  @Override
  public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
    final Iterator<VertexProperty<V>> props;
    if (propertyKeys.length == 1) { // treating as special case for performance
      props = IteratorUtils.of(property(propertyKeys[0]));
    } else if (propertyKeys.length == 0) { // return all properties
      Stream<VertexProperty<V>> vertexPropertyStream = keys().stream().map(this::property);
      props = vertexPropertyStream.iterator();
    } else {
      Stream<VertexProperty<V>> vertexPropertyStream = Arrays.stream(propertyKeys).map(this::property);
      props = vertexPropertyStream.iterator();
    }

    return IteratorUtils.filter(props, p -> p.isPresent());
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

  @Override
  public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
    final Iterator<Node> nodeIterator;
    if (edgeLabels.length == 0) { // tinkerpop convention: follow all available labels
      nodeIterator = allNodes(direction);
    } else {
      nodeIterator = specificNodes(direction, edgeLabels);
    }
    return IteratorUtils.map(nodeIterator, x -> NodeRefTp3.wrap((NodeRef) x));
  }

  private Iterator<Node> allNodes(Direction direction) {
    switch (direction) {
      case OUT: return nodeRef.out();
      case IN: return nodeRef.in();
      case BOTH: return nodeRef.both();
      default: throw new UnsupportedOperationException("unknown direction: " + direction);
    }
  }

  private Iterator<Node> specificNodes(Direction direction, String... labels) {
    switch (direction) {
      case OUT: return nodeRef.out(labels);
      case IN: return nodeRef.in(labels);
      case BOTH: return nodeRef.both(labels);
      default: throw new UnsupportedOperationException("unknown direction: " + direction);
    }
  }

  private Iterator<OdbEdge> allEdges(Direction direction) {
    switch (direction) {
      case OUT: return nodeRef.outE();
      case IN: return nodeRef.inE();
      case BOTH: return nodeRef.bothE();
      default: throw new UnsupportedOperationException("unknown direction: " + direction);
    }
  }

  private Iterator<OdbEdge> specificEdges(Direction direction, String... labels) {
    switch (direction) {
      case OUT: return nodeRef.outE(labels);
      case IN: return nodeRef.inE(labels);
      case BOTH: return nodeRef.bothE(labels);
      default: throw new UnsupportedOperationException("unknown direction: " + direction);
    }
  }

  @Override
  public String toString() {
    return getClass().getName() + "[label=" + label() + "; id=" + id() + "]";
  }
}
