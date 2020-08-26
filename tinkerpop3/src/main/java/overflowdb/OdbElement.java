package overflowdb;

import java.util.Map;
import java.util.Set;

public interface OdbElement {
  String label();

  // TODO drop suffix `2` after tinkerpop interface is gone
  OdbGraph graph2();

  Set<String> propertyKeys();

  // TODO drop suffix `2` after tinkerpop interface is gone
  <P> P property2(String propertyKey);

  /** Map with all properties */
  Map<String, Object> propertyMap();

  void setProperty(String key, Object value);

  void removeProperty(String key);

  void remove();
}
