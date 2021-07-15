package overflowdb.testdomains.configurabledefaults;

import overflowdb.NodeFactory;
import overflowdb.NodeRef;
import overflowdb.Graph;

public class TestNode extends NodeRef<TestNodeDb> {
  public static final String LABEL = "testNode";

  public static final String STRING_PROPERTY = "StringProperty";

  private final String defaultStringPropertyValue;

  public TestNode(Graph graph, long id, String defaultStringPropertyValue) {
    super(graph, id);
    this.defaultStringPropertyValue = defaultStringPropertyValue;
  }

  @Override
  public String label() {
    return TestNode.LABEL;
  }

  public String stringProperty() {
    return get().stringProperty();
  }

  @Override
  public Object propertyDefaultValue(String propertyKey) {
    if (STRING_PROPERTY.equals(propertyKey))
      return defaultStringPropertyValue;
    else
      return super.propertyDefaultValue(propertyKey);
  }

  public static NodeFactory<TestNodeDb> factory(String defaultStringPropertyValue) {
    return new NodeFactory<TestNodeDb>() {
      @Override
      public String forLabel() {
        return TestNode.LABEL;
      }

      @Override
      public TestNodeDb createNode(NodeRef<TestNodeDb> ref) {
        return new TestNodeDb(ref);
      }

      @Override
      public TestNode createNodeRef(Graph graph, long id) {
        return new TestNode(graph, id, defaultStringPropertyValue);
      }
    };
  }

}
