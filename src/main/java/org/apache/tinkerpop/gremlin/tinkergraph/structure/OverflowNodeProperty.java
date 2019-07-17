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

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class OverflowNodeProperty<V> implements Element, VertexProperty<V> {
  private final int id;
  private final Vertex vertex;
  private final String key;
  private final V value;

  public OverflowNodeProperty(final Vertex vertex,
                              final String key,
                              final V value) {
    this(-1, vertex, key, value);
  }

  public OverflowNodeProperty(final int id,
                              final Vertex vertex,
                              final String key,
                              final V value) {
    this.id = id;
    this.vertex = vertex;
    this.key = key;
    this.value = value;
  }

  @Override
  public String key() {
    return key;
  }

  @Override
  public V value() throws NoSuchElementException {
    return value;
  }

  @Override
  public boolean isPresent() {
    return true;
  }

  @Override
  public Vertex element() {
    return vertex;
  }

  @Override
  public Object id() {
    return id;
  }

  @Override
  public <V> Property<V> property(String key, V value) {
    throw new RuntimeException("Not supported.");
  }

  @Override
  public void remove() {
  }

  @Override
  public <U> Iterator<Property<U>> properties(String... propertyKeys) {
    return Collections.emptyIterator();
  }
}
