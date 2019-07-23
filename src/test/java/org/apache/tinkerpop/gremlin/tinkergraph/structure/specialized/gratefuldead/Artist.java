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
package org.apache.tinkerpop.gremlin.tinkergraph.structure.specialized.gratefuldead;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.OverflowDbNode;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.OverflowElementFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.OverflowNodeProperty;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.VertexRef;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.VertexRefWithLabel;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Artist extends OverflowDbNode {
  public static final String label = "artist";

  public static final String NAME = "name";
  public static final Set<String> SPECIFIC_KEYS = new HashSet<>(Arrays.asList(NAME));
  public static final String[] ALLOWED_IN_EDGE_LABELS = {SungBy.label, WrittenBy.label};
  public static final String[] ALLOWED_OUT_EDGE_LABELS = {};

  private static final Map<String, Integer> edgeKeyCount = new HashMap<>();
  private static final Map<String, Integer> edgeLabelAndKeyToPosition = new HashMap<>();
  private static final Map<String, Integer> outEdgeToPosition = new HashMap<>();
  private static final Map<String, Integer> inEdgeToPosition = new HashMap<>();

  static {
    edgeKeyCount.put(SungBy.label, SungBy.SPECIFIC_KEYS.size());
    edgeKeyCount.put(WrittenBy.label, WrittenBy.SPECIFIC_KEYS.size());
    inEdgeToPosition.put(SungBy.label, 0);
    inEdgeToPosition.put(WrittenBy.label, 1);
  }

  /* properties */
  private String name;

  protected Artist(VertexRef ref) {
    super(outEdgeToPosition.size() + inEdgeToPosition.size(), ref);
  }

  public String getName() {
    return name;
  }


  @Override
  protected Set<String> specificKeys() {
    return SPECIFIC_KEYS;
  }

  @Override
  public String[] allowedOutEdgeLabels() {
    return ALLOWED_OUT_EDGE_LABELS;
  }

  @Override
  public String[] allowedInEdgeLabels() {
    return ALLOWED_IN_EDGE_LABELS;
  }

  @Override
  protected int getPositionInEdgeOffsets(Direction direction, String label) {
    final Integer positionOrNull;
    if (direction == Direction.OUT) {
      positionOrNull = outEdgeToPosition.get(label);
    } else {
      positionOrNull = inEdgeToPosition.get(label);
    }
    if (positionOrNull != null) {
      return positionOrNull;
    } else {
      return -1;
    }
  }

  @Override
  protected int getOffsetRelativeToAdjacentVertexRef(String edgeLabel, String key) {
    final Integer offsetOrNull = edgeLabelAndKeyToPosition.get(edgeLabel + key);
    if (offsetOrNull != null) {
      return offsetOrNull;
    } else {
      return -1;
    }
  }

  @Override
  protected int getEdgeKeyCount(String edgeLabel) {
    // TODO handle if it's not allowed
    return edgeKeyCount.get(edgeLabel);
  }

  @Override
  protected List<String> allowedEdgeKeys(String edgeLabel) {
    return new ArrayList<>();
  }

  /* note: usage of `==` (pointer comparison) over `.equals` (String content comparison) is intentional for performance - use the statically defined strings */
  @Override
  protected <V> Iterator<VertexProperty<V>> specificProperties(String key) {
    final VertexProperty<V> ret;
    if (NAME.equals(key) && name != null) {
      return IteratorUtils.of(new OverflowNodeProperty(this, key, name));
    } else {
      return Collections.emptyIterator();
    }
  }

  @Override
  public Map<String, Object> valueMap() {
    Map<String, Object> properties = new HashMap<>();
    if (name != null) properties.put(NAME, name);
    return properties;
  }

  @Override
  protected <V> VertexProperty<V> updateSpecificProperty(
      VertexProperty.Cardinality cardinality, String key, V value) {
    if (NAME.equals(key)) {
      this.name = (String) value;
    } else {
      throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
    }
    return property(key);
  }

  @Override
  protected void removeSpecificProperty(String key) {
    if (NAME.equals(key)) {
      this.name = null;
    } else {
      throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
    }
  }

  public static OverflowElementFactory.ForVertex<Artist> factory = new OverflowElementFactory.ForVertex<Artist>() {

    @Override
    public String forLabel() {
      return Artist.label;
    }

    @Override
    public Artist createVertex(VertexRef<Artist> ref) {
      return new Artist(ref);
    }

    @Override
    public Artist createVertex(Long id, TinkerGraph graph) {
      final VertexRef<Artist> ref = createVertexRef(id, graph);
      final Artist node = createVertex(ref);
      ref.setElement(node);
      return node;
    }

    @Override
    public VertexRef<Artist> createVertexRef(Long id, TinkerGraph graph) {
      return new VertexRefWithLabel<>(id, graph, null, Artist.label);
    }
  };

  @Override
  public String label() {
    return Artist.label;
  }
}
