package overflowdb;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import overflowdb.tinkerpop.OdbProperty;
import overflowdb.util.IteratorUtils;

import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;

public class OdbEdgeTp3 implements Edge {
  public final OdbEdge edge;

  public static OdbEdgeTp3 wrap(OdbEdge edge) {
    return new OdbEdgeTp3(edge);
  }

  private OdbEdgeTp3(OdbEdge edge) {
    this.edge = edge;
  }

  @Override
  public Iterator<Vertex> vertices(org.apache.tinkerpop.gremlin.structure.Direction direction) {
    switch (direction) {
      case OUT:
        return IteratorUtils.from(NodeRefTp3.wrap(edge.outNode()));
      case IN:
        return IteratorUtils.from(NodeRefTp3.wrap(edge.inNode()));
      default:
        return IteratorUtils.from(NodeRefTp3.wrap(edge.outNode()), NodeRefTp3.wrap(edge.inNode()));
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
    return OdbGraphTp3.wrap(edge.graph2());
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
            .map(key -> new OdbProperty<>(key, (V) edge.property2(key), self));
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
