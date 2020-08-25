package overflowdb;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.Iterator;
import java.util.Set;

public class NodeRefTp3<N extends OdbNode> implements Vertex {
  private final NodeRef<N> nodeRef;

  public static <N extends OdbNode> NodeRefTp3 wrap(NodeRef<N> nodeRef) {
    return new NodeRefTp3<>(nodeRef);
  }

  public NodeRefTp3(NodeRef<N> nodeRef) {
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
  public OdbGraph graph() {
    return nodeRef.graph2();
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
    Node inNode = (Node) inVertex;
    final OdbEdge odbEdge = nodeRef.addEdge2(label, inNode, keyValues);
    return OdbEdgeTp3.wrap(odbEdge);
  }

  @Override
  public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... keyValues) {
    return OdbNodeTp3.wrap(nodeRef.get()).property(cardinality, key, value, keyValues);
  }

  @Override
  public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
    return OdbNodeTp3.wrap(nodeRef.get()).properties(propertyKeys);
  }

  @Override
  public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
    return OdbNodeTp3.wrap(nodeRef.get()).edges(direction, edgeLabels);
  }

  @Override
  public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
    return nodes(direction, edgeLabels);
  }

  /* lookup adjacent nodes via direction and labels */
  public Iterator<Vertex> nodes(Direction direction, String... edgeLabels) {
    return OdbNodeTp3.wrap(nodeRef.get()).nodes(direction, edgeLabels);
  }

  @Override
  public String toString() {
    return getClass().getName() + "[label=" + label() + "; id=" + id() + "]";
  }
}
