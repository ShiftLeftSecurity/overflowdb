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

import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.NodeLayoutInformation;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.OverflowDbNode;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.OverflowElementFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.OverflowNodeProperty;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.VertexRef;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.VertexRefWithLabel;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class Artist extends OverflowDbNode {
  public static final String label = "artist";
  public static final String NAME = "name";

  /* properties */
  private String name;

  protected Artist(VertexRef ref) {
    super(ref);
  }

  @Override
  public String label() {
    return Artist.label;
  }

  @Override
  protected NodeLayoutInformation layoutInformation() {
    return layoutInformation;
  }

  public String getName() {
    return name;
  }

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

  private static NodeLayoutInformation layoutInformation = new NodeLayoutInformation(
      new HashSet<>(Arrays.asList(NAME)),
      Arrays.asList(),
      Arrays.asList(SungBy.layoutInformation, WrittenBy.layoutInformation));

  public static OverflowElementFactory.ForNode<Artist> factory = new OverflowElementFactory.ForNode<Artist>() {

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
}
