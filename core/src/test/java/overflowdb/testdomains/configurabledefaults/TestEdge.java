package overflowdb.testdomains.configurabledefaults;

import overflowdb.EdgeFactory;
import overflowdb.EdgeLayoutInformation;
import overflowdb.NodeRef;
import overflowdb.Edge;
import overflowdb.Graph;

import java.util.Arrays;
import java.util.HashSet;

public class TestEdge extends Edge {
  public static final String LABEL = "testEdge";
  public static final String LONG_PROPERTY = "longProperty";
  public static final HashSet<String> PROPERTY_KEYS = new HashSet<>(Arrays.asList(LONG_PROPERTY));
  private final long defaultLongPropertyValue;

  public TestEdge(Graph graph, NodeRef outVertex, NodeRef inVertex, long defaultLongPropertyValue) {
    super(graph, LABEL, outVertex, inVertex, PROPERTY_KEYS);
    this.defaultLongPropertyValue = defaultLongPropertyValue;
  }

  public Long longProperty() {
    return (Long) property(LONG_PROPERTY);
  }

  @Override
  public Object propertyDefaultValue(String propertyKey) {
    if (LONG_PROPERTY.equals(propertyKey))
      return defaultLongPropertyValue;
    else
      return super.propertyDefaultValue(propertyKey);
  }

  public static final EdgeLayoutInformation layoutInformation = new EdgeLayoutInformation(LABEL, PROPERTY_KEYS);

  public static EdgeFactory<TestEdge> factory(long defaultLongPropertyValue) {
    return new EdgeFactory<TestEdge>() {
      @Override
      public String forLabel() {
        return TestEdge.LABEL;
      }

      @Override
      public TestEdge createEdge(Graph graph, NodeRef outVertex, NodeRef inVertex) {
        return new TestEdge(graph, outVertex, inVertex, defaultLongPropertyValue);
      }
    };
  }

}
