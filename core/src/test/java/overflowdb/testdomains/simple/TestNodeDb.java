package overflowdb.testdomains.simple;

import overflowdb.NodeLayoutInformation;
import overflowdb.NodeRef;
import overflowdb.NodeDb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class TestNodeDb extends NodeDb {
  protected TestNodeDb(NodeRef ref) {
    super(ref);
  }

  private String _stringProperty;
  private Integer _intProperty;
  private List<String> _stringListProperty;
  private List<Integer> _intListProperty;
  private FunkyList _funkyList;

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
    return _intListProperty;
  }

  public FunkyList funkyList() {
    return _funkyList;
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
      return intListProperty();
    } else if (key == TestNode.FUNKY_LIST_PROPERTY) {
      return funkyList();
    } else {
      return propertyDefaultValue(key);
    }
  }

  @Override
  protected void updateSpecificProperty(String key, Object value) {
    if (TestNode.STRING_PROPERTY.equals(key)) {
      this._stringProperty = (String) value;
    } else if (TestNode.STRING_LIST_PROPERTY.equals(key)) {
      if (value instanceof List) {
        this._stringListProperty = (List) value;
      } else {
        if (this._stringListProperty == null) this._stringListProperty = new ArrayList<>();
        this._stringListProperty.add((String) value);
      }
    } else if (TestNode.INT_PROPERTY.equals(key)) {
      this._intProperty = (Integer) value;
    } else if (TestNode.INT_LIST_PROPERTY.equals(key)) {
      if (value instanceof List) {
        this._intListProperty = (List) value;
      } else {
        if (this._intListProperty == null) this._intListProperty = new ArrayList<>();
        this._intListProperty.add((Integer) value);
      }
    } else if (TestNode.FUNKY_LIST_PROPERTY.equals(key)) {
      if (value instanceof String[]) {
        String[] valueAsStrings = (String[]) value;
        this._funkyList = new FunkyList();
        for (String entry : valueAsStrings) {
          _funkyList.add(entry);
        }
      } else {
        this._funkyList = (FunkyList) value;
      }
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
    } else {
      throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
    }
  }

  public static NodeLayoutInformation layoutInformation = new NodeLayoutInformation(
      TestNode.LABEL,
      new HashSet<>(Arrays.asList(TestNode.STRING_PROPERTY, TestNode.INT_PROPERTY, TestNode.STRING_LIST_PROPERTY, TestNode.INT_LIST_PROPERTY, TestNode.FUNKY_LIST_PROPERTY)),
      Arrays.asList(TestEdge.layoutInformation),
      Arrays.asList(TestEdge.layoutInformation));

}
