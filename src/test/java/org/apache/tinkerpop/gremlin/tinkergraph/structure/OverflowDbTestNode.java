/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.tinkergraph.structure;

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

public class OverflowDbTestNode extends OverflowDbNode {
  public static final String label = "testNode";

  public static final String STRING_PROPERTY = "StringProperty";
  public static final String INT_PROPERTY = "IntProperty";
  public static final String STRING_LIST_PROPERTY = "StringListProperty";
  public static final String INT_LIST_PROPERTY = "IntListProperty";

  /* properties */
  private String stringProperty;
  private Integer intProperty;
  private List<String> stringListProperty;
  private List<Integer> intListProperty;

  protected OverflowDbTestNode(VertexRef ref) {
    super(ref);
  }

  @Override
  public String label() {
    return OverflowDbTestNode.label;
  }

  @Override
  protected NodeLayoutInformation layoutInformation() {
    return layoutInformation;
  }

  /* note: usage of `==` (pointer comparison) over `.equals` (String content comparison) is intentional for performance - use the statically defined strings */
  @Override
  protected <V> Iterator<VertexProperty<V>> specificProperties(String key) {
    if (STRING_PROPERTY.equals(key) && stringProperty != null) {
      return IteratorUtils.of(new OverflowNodeProperty(this, key, stringProperty));
    } else if (key == STRING_LIST_PROPERTY && stringListProperty != null) {
      return IteratorUtils.of(new OverflowNodeProperty(this, key, stringListProperty));
    } else if (key == INT_PROPERTY && intProperty != null) {
      return IteratorUtils.of(new OverflowNodeProperty(this, key, intProperty));
    } else if (key == INT_LIST_PROPERTY && intListProperty != null) {
      return IteratorUtils.of(new OverflowNodeProperty(this, key, intListProperty));
    } else {
      return Collections.emptyIterator();
    }
  }

  @Override
  public Map<String, Object> valueMap() {
    Map<String, Object> properties = new HashMap<>();
    if (stringProperty != null) properties.put(STRING_PROPERTY, stringProperty);
    if (stringListProperty != null) properties.put(STRING_LIST_PROPERTY, stringListProperty);
    if (intProperty != null) properties.put(INT_PROPERTY, intProperty);
    if (intListProperty != null) properties.put(INT_LIST_PROPERTY, intListProperty);
    return properties;
  }

  @Override
  protected <V> VertexProperty<V> updateSpecificProperty(
      VertexProperty.Cardinality cardinality, String key, V value) {
    if (STRING_PROPERTY.equals(key)) {
      this.stringProperty = (String) value;
    } else if (STRING_LIST_PROPERTY.equals(key)) {
      if (value instanceof List) {
        this.stringListProperty = (List) value;
      } else {
        if (this.stringListProperty == null) this.stringListProperty = new ArrayList<>();
        this.stringListProperty.add((String) value);
      }
    } else if (INT_PROPERTY.equals(key)) {
      this.intProperty = (Integer) value;
    } else if (INT_LIST_PROPERTY.equals(key)) {
      if (value instanceof List) {
        this.intListProperty = (List) value;
      } else {
        if (this.intListProperty == null) this.intListProperty = new ArrayList<>();
        this.intListProperty.add((Integer) value);
      }
    } else {
      throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
    }
    return property(key);
  }

  @Override
  protected void removeSpecificProperty(String key) {
    if (STRING_PROPERTY.equals(key)) {
      this.stringProperty = null;
    } else if (STRING_LIST_PROPERTY.equals(key)) {
      this.stringListProperty = null;
    } else if (INT_PROPERTY.equals(key)) {
      this.intProperty = null;
    } else if (INT_LIST_PROPERTY.equals(key)) {
      this.intListProperty = null;
    } else {
      throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
    }
  }

  private static NodeLayoutInformation layoutInformation = new NodeLayoutInformation(
      new HashSet<>(Arrays.asList(STRING_PROPERTY, INT_PROPERTY, STRING_LIST_PROPERTY, INT_LIST_PROPERTY)),
      Arrays.asList(OverflowDbTestEdge.layoutInformation),
      Arrays.asList(OverflowDbTestEdge.layoutInformation));

  public static OverflowElementFactory.ForNode<OverflowDbTestNode> factory = new OverflowElementFactory.ForNode<OverflowDbTestNode>() {

    @Override
    public String forLabel() {
      return OverflowDbTestNode.label;
    }

    @Override
    public OverflowDbTestNode createVertex(VertexRef<OverflowDbTestNode> ref) {
      return new OverflowDbTestNode(ref);
    }

    @Override
    public OverflowDbTestNode createVertex(Long id, TinkerGraph graph) {
      final VertexRef<OverflowDbTestNode> ref = createVertexRef(id, graph);
      final OverflowDbTestNode node = createVertex(ref);
      ref.setElement(node);
      return node;
    }

    @Override
    public VertexRef<OverflowDbTestNode> createVertexRef(Long id, TinkerGraph graph) {
      return new VertexRefWithLabel<>(id, graph, null, OverflowDbTestNode.label);
    }
  };

}
