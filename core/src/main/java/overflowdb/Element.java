package overflowdb;

import java.util.Map;
import java.util.Set;

public interface Element {
  String label();

  Graph graph();

  Set<String> propertyKeys();

  Object property(String propertyKey);

  /** Map with all properties */
  Map<String, Object> propertyMap();

  void setProperty(String key, Object value);

  void removeProperty(String key);

  void remove();
}
