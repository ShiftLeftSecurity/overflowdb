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

  <P> void setProperty(String key, P value);

  void remove();
}
