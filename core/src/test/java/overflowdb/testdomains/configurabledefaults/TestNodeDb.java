package overflowdb.testdomains.configurabledefaults;

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

  public String stringProperty() {
    if (_stringProperty != null)
      return _stringProperty;
    else
      return (String) propertyDefaultValue(TestNode.STRING_PROPERTY);
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
    } else {
      return propertyDefaultValue(key);
    }
  }

  @Override
  public Map<String, Object> valueMap() {
    Map<String, Object> properties = new HashMap<>();
    if (_stringProperty != null) properties.put(TestNode.STRING_PROPERTY, _stringProperty);
    return properties;
  }

  @Override
  protected void updateSpecificProperty(String key, Object value) {
    if (TestNode.STRING_PROPERTY.equals(key)) {
      this._stringProperty = (String) value;
    } else {
      throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
    }
  }

  @Override
  protected void removeSpecificProperty(String key) {
    if (TestNode.STRING_PROPERTY.equals(key)) {
      this._stringProperty = null;
    } else {
      throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
    }
  }

  public static NodeLayoutInformation layoutInformation = new NodeLayoutInformation(
      TestNode.LABEL,
      new HashSet<>(Arrays.asList(TestNode.STRING_PROPERTY)),
      Arrays.asList(TestEdge.layoutInformation),
      Arrays.asList(TestEdge.layoutInformation));
}
