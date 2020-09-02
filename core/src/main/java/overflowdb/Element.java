package overflowdb;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface Element {
  String label();

  Graph graph();

  Set<String> propertyKeys();

  Object property(String key);

  <A> A property(PropertyKey<A> key);

  <A> Optional<A> propertyOption(PropertyKey<A> key);

  Optional<Object> propertyOption(String key);

  /** Map with all properties */
  Map<String, Object> propertyMap();

  void setProperty(String key, Object value);

  <A> void setProperty(PropertyKey<A> key, A value);

  void setProperty(Property<?> property);

  void removeProperty(String key);

  void remove();
}
