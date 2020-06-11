package overflowdb.testdomains.simple;

import overflowdb.NodeLayoutInformation;
import overflowdb.NodeRef;
import overflowdb.OdbNode;
import overflowdb.OdbNodeProperty;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class TestNodeDb extends OdbNode {
  protected TestNodeDb(NodeRef ref) {
    super(ref);
  }

  private String _stringProperty;
  private Integer _intProperty;
  private List<String> _stringListProperty;
  private List<Integer> _intListProperty;

  public String stringProperty() {
    return _stringProperty;
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

  @Override
  public NodeLayoutInformation layoutInformation() {
    return layoutInformation;
  }

  @Override
  protected <V> Iterator<VertexProperty<V>> specificProperties(String key) {
    final Object value = specificProperty2(key);
    if (value != null) return IteratorUtils.of(new OdbNodeProperty(this, key, value));
    else return Collections.emptyIterator();
  }

  /* note: usage of `==` (pointer comparison) over `.equals` (String content comparison) is intentional for performance - use the statically defined strings */
  @Override
  protected Object specificProperty2(String key) {
    if (TestNode.STRING_PROPERTY.equals(key)) {
      return _stringProperty;
    } else if (key == TestNode.STRING_LIST_PROPERTY) {
      return _stringListProperty;
    } else if (key == TestNode.INT_PROPERTY) {
      return _intProperty;
    } else if (key == TestNode.INT_LIST_PROPERTY) {
      return _intListProperty;
    } else {
      return null;
    }
  }

  @Override
  public Map<String, Object> valueMap() {
    Map<String, Object> properties = new HashMap<>();
    if (_stringProperty != null) properties.put(TestNode.STRING_PROPERTY, _stringProperty);
    if (_stringListProperty != null) properties.put(TestNode.STRING_LIST_PROPERTY, _stringListProperty);
    if (_intProperty != null) properties.put(TestNode.INT_PROPERTY, _intProperty);
    if (_intListProperty != null) properties.put(TestNode.INT_LIST_PROPERTY, _intListProperty);
    return properties;
  }

  @Override
  protected <V> VertexProperty<V> updateSpecificProperty(
      VertexProperty.Cardinality cardinality, String key, V value) {
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
    } else {
      throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
    }
    return property(key);
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
    } else {
      throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
    }
  }

  public static NodeLayoutInformation layoutInformation = new NodeLayoutInformation(
      2,
      new HashSet<>(Arrays.asList(TestNode.STRING_PROPERTY, TestNode.INT_PROPERTY, TestNode.STRING_LIST_PROPERTY, TestNode.INT_LIST_PROPERTY)),
      Arrays.asList(TestEdge.layoutInformation),
      Arrays.asList(TestEdge.layoutInformation));
}
