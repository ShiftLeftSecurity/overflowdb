package overflowdb.testdomains.simple;

import overflowdb.NodeFactory;
import overflowdb.NodeRef;
import overflowdb.Graph;

import java.util.List;

public class TestNode extends NodeRef<TestNodeDb> {
  public static final String LABEL = "testNode";

  public static final String STRING_PROPERTY = "StringProperty";
  public static final String INT_PROPERTY = "IntProperty";
  public static final String STRING_LIST_PROPERTY = "StringListProperty";
  public static final String INT_LIST_PROPERTY = "IntListProperty";

  public TestNode(Graph graph, long id) {
    super(graph, id);
  }

  @Override
  public String label() {
    return TestNode.LABEL;
  }

  public String stringProperty() {
    return get().stringProperty();
  }

  public Integer intProperty() {
    return get().intProperty();
  }

  public List<String> stringListProperty() {
    return get().stringListProperty();
  }

  public List<Integer> intListProperty() {
    return get().intListProperty();
  }

  public static NodeFactory<TestNodeDb> factory = new NodeFactory<TestNodeDb>() {

    @Override
    public String forLabel() {
      return TestNode.LABEL;
    }

    @Override
    public int forLabelId() {
      return TestNodeDb.layoutInformation.labelId;
    }

    @Override
    public TestNodeDb createNode(NodeRef<TestNodeDb> ref) {
      return new TestNodeDb(ref);
    }

    @Override
    public TestNode createNodeRef(Graph graph, long id) {
      return new TestNode(graph, id);
    }
  };

}
