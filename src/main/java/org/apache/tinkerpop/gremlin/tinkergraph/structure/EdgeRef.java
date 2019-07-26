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

import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.io.IOException;
import java.util.Iterator;

public abstract class EdgeRef<E extends Edge> extends ElementRef<E> implements Edge {

  public EdgeRef(final Object edgeId, final Graph graph, E edge) {
    super(edgeId, graph, edge);
  }

  @Override
  protected E readFromDisk(final long edgeId) throws IOException {
    return graph.ondiskOverflow.readEdge(edgeId);
  }

  @Override
  public String toString() {
    return StringFactory.edgeString(this);
  }

  // delegate methods start
  @Override
  public <V> Property<V> property(String key, V value) {
    return this.get().property(key, value);
  }

  @Override
  public Iterator<Vertex> vertices(Direction direction) {
    return this.get().vertices(direction);
  }

  @Override
  public <V> Iterator<Property<V>> properties(String... propertyKeys) {
    return this.get().properties(propertyKeys);
  }
  // delegate methods end
}
