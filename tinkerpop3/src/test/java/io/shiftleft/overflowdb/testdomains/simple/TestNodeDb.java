package io.shiftleft.overflowdb.testdomains.simple;

import io.shiftleft.overflowdb.NodeLayoutInformation;
import io.shiftleft.overflowdb.NodeRef;
import io.shiftleft.overflowdb.OdbNode;
import io.shiftleft.overflowdb.OdbNodeProperty;
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

import static io.shiftleft.overflowdb.testdomains.simple.TestNode.INT_LIST_PROPERTY;
import static io.shiftleft.overflowdb.testdomains.simple.TestNode.INT_PROPERTY;
import static io.shiftleft.overflowdb.testdomains.simple.TestNode.LABEL;
import static io.shiftleft.overflowdb.testdomains.simple.TestNode.STRING_LIST_PROPERTY;
import static io.shiftleft.overflowdb.testdomains.simple.TestNode.STRING_PROPERTY;

public class TestNodeDb extends OdbNode {
  protected TestNodeDb(NodeRef ref) {
    super(ref);
  }

  @Override
  public String label() {
    return LABEL;
  }

  /**
   * properties
   */
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
  protected NodeLayoutInformation layoutInformation() {
    return layoutInformation;
  }

  /* note: usage of `==` (pointer comparison) over `.equals` (String content comparison) is intentional for performance - use the statically defined strings */
  @Override
  protected <V> Iterator<VertexProperty<V>> specificProperties(String key) {
    if (STRING_PROPERTY.equals(key) && _stringProperty != null) {
      return IteratorUtils.of(new OdbNodeProperty(this, key, _stringProperty));
    } else if (key == STRING_LIST_PROPERTY && _stringListProperty != null) {
      return IteratorUtils.of(new OdbNodeProperty(this, key, _stringListProperty));
    } else if (key == INT_PROPERTY && _intProperty != null) {
      return IteratorUtils.of(new OdbNodeProperty(this, key, _intProperty));
    } else if (key == INT_LIST_PROPERTY && _intListProperty != null) {
      return IteratorUtils.of(new OdbNodeProperty(this, key, _intListProperty));
    } else {
      return Collections.emptyIterator();
    }
  }

  @Override
  public Map<String, Object> valueMap() {
    Map<String, Object> properties = new HashMap<>();
    if (_stringProperty != null) properties.put(STRING_PROPERTY, _stringProperty);
    if (_stringListProperty != null) properties.put(STRING_LIST_PROPERTY, _stringListProperty);
    if (_intProperty != null) properties.put(INT_PROPERTY, _intProperty);
    if (_intListProperty != null) properties.put(INT_LIST_PROPERTY, _intListProperty);
    return properties;
  }

  @Override
  protected <V> VertexProperty<V> updateSpecificProperty(
      VertexProperty.Cardinality cardinality, String key, V value) {
    if (STRING_PROPERTY.equals(key)) {
      this._stringProperty = (String) value;
    } else if (STRING_LIST_PROPERTY.equals(key)) {
      if (value instanceof List) {
        this._stringListProperty = (List) value;
      } else {
        if (this._stringListProperty == null) this._stringListProperty = new ArrayList<>();
        this._stringListProperty.add((String) value);
      }
    } else if (INT_PROPERTY.equals(key)) {
      this._intProperty = (Integer) value;
    } else if (INT_LIST_PROPERTY.equals(key)) {
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
    if (STRING_PROPERTY.equals(key)) {
      this._stringProperty = null;
    } else if (STRING_LIST_PROPERTY.equals(key)) {
      this._stringListProperty = null;
    } else if (INT_PROPERTY.equals(key)) {
      this._intProperty = null;
    } else if (INT_LIST_PROPERTY.equals(key)) {
      this._intListProperty = null;
    } else {
      throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
    }
  }

  private static NodeLayoutInformation layoutInformation = new NodeLayoutInformation(
      new HashSet<>(Arrays.asList(STRING_PROPERTY, INT_PROPERTY, STRING_LIST_PROPERTY, INT_LIST_PROPERTY)),
      Arrays.asList(TestEdge.layoutInformation),
      Arrays.asList(TestEdge.layoutInformation));
}
