package overflowdb;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import overflowdb.tinkerpop.OdbProperty;
import overflowdb.util.IteratorUtils;

import java.util.Iterator;
import java.util.Set;

public class OdbEdgeTp3 implements Edge {
  private final OdbEdge wrapped;

  public OdbEdgeTp3(OdbEdge wrapped) {
    this.wrapped = wrapped;
  }

  @Override
  public Iterator<Vertex> vertices(org.apache.tinkerpop.gremlin.structure.Direction direction) {
    switch (direction) {
      case OUT:
        return IteratorUtils.from(wrapped.outNode());
      case IN:
        return IteratorUtils.from(wrapped.inNode());
      default:
        return IteratorUtils.from(wrapped.outNode(), wrapped.inNode());
    }
  }

  @Override
  public Object id() {
    return this;
  }

  @Override
  public String label() {
    return wrapped.label();
  }

  @Override
  public Graph graph() {
    return wrapped.graph2();
  }

  @Override
  public <V> Property<V> property(String key, V value) {
    wrapped.setProperty(key, value);
    return new OdbProperty<>(key, value, this);
  }

  @Override
  public Set<String> keys() {
    return wrapped.propertyKeys();
  }

  @Override
  public void remove() {
    wrapped.remove();
  }

  @Override
  public <V> Iterator<Property<V>> properties(String... propertyKeys) {
    if (wrapped.isInBlockOffsetInitialized()) {
      return wrapped.inNode().get().getEdgeProperties(Direction.IN, this, wrapped.getInBlockOffset(), propertyKeys);
    } else if (wrapped.isOutBlockOffsetInitialized()) {
      return wrapped.outNode().get().getEdgeProperties(Direction.OUT, this, wrapped.getOutBlockOffset(), propertyKeys);
    } else {
      throw new RuntimeException("Cannot get properties. In and out block offset uninitialized.");
    }
  }

  @Override
  public <V> Property<V> property(String propertyKey) {
    if (wrapped.isInBlockOffsetInitialized()) {
      return wrapped.inNode().get().getEdgeProperty(Direction.IN, this, wrapped.getInBlockOffset(), propertyKey);
    } else if (wrapped.isOutBlockOffsetInitialized()) {
      return wrapped.outNode().get().getEdgeProperty(Direction.OUT, this, wrapped.getOutBlockOffset(), propertyKey);
    } else {
      throw new RuntimeException("Cannot get property. In and out block offset unitialized.");
    }
  }

  public boolean isRemoved() {
    return wrapped.isRemoved();
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof OdbEdgeTp3)) {
      return false;
    }

    OdbEdgeTp3 otherEdge = (OdbEdgeTp3) other;
    return wrapped.equals(otherEdge.wrapped);
  }

  @Override
  public int hashCode() {
    return wrapped.hashCode();
  }

}
