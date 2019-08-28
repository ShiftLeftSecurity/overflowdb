package io.shiftleft.overflowdb.testdomains.movielens;

import io.shiftleft.overflowdb.NodeLayoutInformation;
import io.shiftleft.overflowdb.NodeRef;
import io.shiftleft.overflowdb.OdbNode;
import io.shiftleft.overflowdb.OdbNodeProperty;
import io.shiftleft.overflowdb.testdomains.simple.TestEdge;
import io.shiftleft.overflowdb.testdomains.simple.TestNode;
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

public class OccupationDb extends OdbNode {
  protected OccupationDb(NodeRef ref) {
    super(ref);
  }

  private String _name;
  private String _uid;

  public String name() {
    return _name;
  }

  public String uid() {
    return _uid;
  }

  @Override
  protected NodeLayoutInformation layoutInformation() {
    return layoutInformation;
  }

  /* note: usage of `==` (pointer comparison) over `.equals` (String content comparison) is intentional for performance - use the statically defined strings */
  @Override
  protected <V> Iterator<VertexProperty<V>> specificProperties(String key) {
    if (Occupation.NAME.equals(key) && _name != null) {
      return IteratorUtils.of(new OdbNodeProperty(this, key, _name));
    } else if (key == Occupation.UID && _uid != null) {
      return IteratorUtils.of(new OdbNodeProperty(this, key, _uid));
    } else {
      return Collections.emptyIterator();
    }
  }

  @Override
  public Map<String, Object> valueMap() {
    Map<String, Object> properties = new HashMap<>();
    if (_name != null) properties.put(Occupation.NAME, _name);
    if (_uid != null) properties.put(Occupation.UID, _uid);
    return properties;
  }

  @Override
  protected <V> VertexProperty<V> updateSpecificProperty(
      VertexProperty.Cardinality cardinality, String key, V value) {
    if (Occupation.NAME.equals(key)) {
      this._name = (String) value;
    } else if (Occupation.UID.equals(key)) {
      this._uid = (String) value;
    } else {
      throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
    }
    return property(key);
  }

  @Override
  protected void removeSpecificProperty(String key) {
    if (Occupation.NAME.equals(key)) {
      this._name = null;
    } else if (Occupation.UID.equals(key)) {
      this._uid = null;
    } else {
      throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
    }
  }

  private static NodeLayoutInformation layoutInformation = new NodeLayoutInformation(
      new HashSet<>(Arrays.asList(Occupation.NAME, Occupation.UID, TestNode.STRING_LIST_PROPERTY, TestNode.INT_LIST_PROPERTY)),
      Arrays.asList(TestEdge.layoutInformation),
      Arrays.asList(TestEdge.layoutInformation));
}
