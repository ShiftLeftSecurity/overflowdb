package overflowdb;

public interface OdbElement {
  String label();

  // TODO drop suffix `2` after tinkerpop interface is gone
  OdbGraph graph2();

  // TODO drop suffix `2` after tinkerpop interface is gone
  <P> P property2(String propertyKey);

  <P> void setProperty(String key, P value);
}
