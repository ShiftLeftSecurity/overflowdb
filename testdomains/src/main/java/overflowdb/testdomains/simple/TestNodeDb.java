package overflowdb.testdomains.simple;

import overflowdb.NodeLayoutInformation;
import overflowdb.NodeRef;
import overflowdb.NodeDb;

import java.util.*;
import java.util.stream.Collectors;

public class TestNodeDb extends NodeDb {
  protected TestNodeDb(NodeRef ref) {
    super(ref);
  }

  private String _stringProperty;
  private Integer _intProperty;
  private List<String> _stringListProperty;
  private int[] _intListProperty; // to test primitive arrays serialization
  private FunkyList _funkyList;
  private TestNode _containedTestNode;

  public String stringProperty() {
    if (_stringProperty != null)
      return _stringProperty;
    else
      return (String) propertyDefaultValue(TestNode.STRING_PROPERTY);
  }

  public Integer intProperty() {
    return _intProperty;
  }

  public List<String> stringListProperty() {
    return _stringListProperty;
  }

  public List<Integer> intListProperty() {
    return Arrays.stream(_intListProperty).boxed().collect(Collectors.toList());
  }

  public FunkyList funkyList() {
    return _funkyList;
  }

  public TestNode containedTestNode() {
    return _containedTestNode;
  }

  @Override
  public NodeLayoutInformation layoutInformation() {
    return layoutInformation;
  }

  /* note: usage of `==` (pointer comparison) over `.equals` (String content comparison) is intentional for performance - use the statically defined strings */
  @Override
  public Object property(String key) {
    if (TestNode.STRING_PROPERTY.equals(key)) {
      return stringProperty();
    } else if (key == TestNode.STRING_LIST_PROPERTY) {
      return stringListProperty();
    } else if (key == TestNode.INT_PROPERTY) {
      return intProperty();
    } else if (key == TestNode.INT_LIST_PROPERTY) {
      return _intListProperty;
    } else if (key == TestNode.FUNKY_LIST_PROPERTY) {
      return funkyList();
    } else if (key == TestNode.CONTAINED_TESTNODE_PROPERTY) {
      return containedTestNode();
    } else {
      return propertyDefaultValue(key);
    }
  }

  @Override
  protected void updateSpecificProperty(String key, Object value) {
    if (TestNode.STRING_PROPERTY.equals(key)) {
      this._stringProperty = (String) value;
    } else if (TestNode.STRING_LIST_PROPERTY.equals(key)) {
      if (value instanceof Collection) {
        this._stringListProperty = new ArrayList<>((Collection) value);
      } else if (value instanceof Object[]) {
        this._stringListProperty = Arrays.stream((Object[]) value).map(s -> (String) s).collect(Collectors.toList());
      } else {
        if (this._stringListProperty == null) this._stringListProperty = new ArrayList<>();
        this._stringListProperty.add((String) value);
      }
    } else if (TestNode.INT_PROPERTY.equals(key)) {
      this._intProperty = (Integer) value;
    } else if (TestNode.INT_LIST_PROPERTY.equals(key)) {
      if (value instanceof Collection) {
        Collection<Integer> valueAsList = (Collection) value;
        this._intListProperty = new int[valueAsList.size()];
        int idx = 0;
        for (Integer i : valueAsList) _intListProperty[idx++] = i;
      } else if (value instanceof int[]) {
        this._intListProperty = (int[]) value;
      } else if (value instanceof Object[]) {
        Object[] value1 = (Object[]) value;
        this._intListProperty = new int[value1.length];
        for (int i = 0; i < value1.length; i++)
          this._intListProperty[i] = (int) value1[i];
      } else {
        throw new RuntimeException("not implemented... " + value.getClass());
      }
    } else if (TestNode.FUNKY_LIST_PROPERTY.equals(key)) {
      if (value instanceof Object[]) {
        Object[] values = (Object[]) value;
        this._funkyList = new FunkyList();
        for (Object entry : values) {
          _funkyList.add((String) entry);
        }
      } else if (value instanceof Iterable) {
        this._funkyList = new FunkyList();
        ((Iterable<String>) value).iterator().forEachRemaining(_funkyList::add);
      } else if (value instanceof String) {
        this._funkyList = new FunkyList();
        _funkyList.add((String) value);
      } else {
        this._funkyList = (FunkyList) value;
      }
    } else if (TestNode.CONTAINED_TESTNODE_PROPERTY.equals(key)) {
      this._containedTestNode = (TestNode) value;
    } else {
      throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
    }
  }

  @Override
  protected void removeSpecificProperty(String key) {
    if (TestNode.STRING_PROPERTY.equals(key)) {
      this._stringProperty = null;
    } else if (TestNode.STRING_LIST_PROPERTY.equals(key)) {
      this._stringListProperty = null;
    } else if (TestNode.INT_PROPERTY.equals(key)) {
      this._intProperty = null;
    } else if (TestNode.INT_LIST_PROPERTY.equals(key)) {
      this._intListProperty = null;
    } else if (TestNode.FUNKY_LIST_PROPERTY.equals(key)) {
      this._funkyList = null;
    } else if (TestNode.CONTAINED_TESTNODE_PROPERTY.equals(key)) {
      this._containedTestNode = null;
    } else {
      throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
    }
  }

  public static NodeLayoutInformation layoutInformation = new NodeLayoutInformation(
      TestNode.LABEL,
      new HashSet<>(Arrays.asList(TestNode.STRING_PROPERTY, TestNode.INT_PROPERTY, TestNode.STRING_LIST_PROPERTY, TestNode.INT_LIST_PROPERTY, TestNode.FUNKY_LIST_PROPERTY, TestNode.CONTAINED_TESTNODE_PROPERTY)),
      Arrays.asList(TestEdge.layoutInformation),
      Arrays.asList(TestEdge.layoutInformation));

}
