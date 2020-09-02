package overflowdb.tinkerpop;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import overflowdb.Edge;
import overflowdb.util.IteratorUtils;

import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;

public class OdbEdgeTp3 implements org.apache.tinkerpop.gremlin.structure.Edge {
  public final Edge edge;

  public static OdbEdgeTp3 wrap(Edge edge) {
    return new OdbEdgeTp3(edge);
  }

  private OdbEdgeTp3(Edge edge) {
    this.edge = edge;
  }

  @Override
  public Iterator<Vertex> vertices(org.apache.tinkerpop.gremlin.structure.Direction direction) {
    switch (direction) {
      case OUT:
        return IteratorUtils.from(NodeTp3.wrap(edge.outNode()));
      case IN:
        return IteratorUtils.from(NodeTp3.wrap(edge.inNode()));
      default:
        return IteratorUtils.from(NodeTp3.wrap(edge.outNode()), NodeTp3.wrap(edge.inNode()));
    }
  }

  @Override
  public Object id() {
    return this;
  }

  @Override
  public String label() {
    return edge.label();
  }

  @Override
  public Graph graph() {
    return OdbGraphTp3.wrap(edge.graph());
  }

  @Override
  public <V> Property<V> property(String key, V value) {
    edge.setProperty(key, value);
    return new OdbProperty<>(key, value, this);
  }

  @Override
  public Set<String> keys() {
    return edge.propertyKeys();
  }

  @Override
  public void remove() {
    edge.remove();
  }

  @Override
  public <V> Iterator<Property<V>> properties(String... propertyKeys) {
    OdbEdgeTp3 self = this;
    final Stream<Property<V>> stream =
      Stream.of(propertyKeys)
            .map(key -> new OdbProperty<>(key, (V) edge.property(key), self));
    return stream.iterator();
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof OdbEdgeTp3)) {
      return false;
    }

    OdbEdgeTp3 otherEdge = (OdbEdgeTp3) other;
    return edge.equals(otherEdge.edge);
  }

  @Override
  public int hashCode() {
    return edge.hashCode();
  }

}
