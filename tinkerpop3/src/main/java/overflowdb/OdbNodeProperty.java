package overflowdb;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class OdbNodeProperty<V> implements Element, VertexProperty<V> {
  private final int id;
  private final Vertex vertex;
  private final String key;
  private final V value;

  public OdbNodeProperty(final Vertex vertex,
                         final String key,
                         final V value) {
    this(-1, vertex, key, value);
  }

  public OdbNodeProperty(final int id,
                         final Vertex vertex,
                         final String key,
                         final V value) {
    this.id = id;
    this.vertex = vertex;
    this.key = key;
    this.value = value;
  }

  @Override
  public String key() {
    return key;
  }

  @Override
  public V value() throws NoSuchElementException {
    return value;
  }

  @Override
  public boolean isPresent() {
    return true;
  }

  @Override
  public Vertex element() {
    return vertex;
  }

  @Override
  public Object id() {
    return id;
  }

  @Override
  public <V> Property<V> property(String key, V value) {
    throw new RuntimeException("Not supported.");
  }

  @Override
  public void remove() {
    ((OdbNode) vertex).removeSpecificProperty(key);
  }

  @Override
  public <U> Iterator<Property<U>> properties(String... propertyKeys) {
    return Collections.emptyIterator();
  }
}
