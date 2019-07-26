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
package org.apache.tinkerpop.gremlin.tinkergraph.storage;

import gnu.trove.map.hash.THashMap;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;
import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;

import java.util.Arrays;
import java.util.Map;

public class EdgeSerializer extends Serializer<Edge> {

  @Override
  protected long getId(Edge edge) {
    return (long) edge.id();
  }

  @Override
  protected String getLabel(Edge edge) {
    return edge.label();
  }

  @Override
  protected Map<String, Object> getProperties(Edge edge) {
    Map<String, Object> properties = new THashMap<>();
    edge.properties().forEachRemaining(property -> properties.put(property.key(), property.value()));
    return properties;
  }

  @Override
  /** using same format to store edgeIds as for vertices */
  protected Map<String, TLongSet> getEdgeIds(Edge edge, Direction direction) {
    final Map<String, TLongSet> edgeIds = new THashMap<>();
    switch (direction) {
      case IN:
        edgeIds.put(Direction.IN.name(), new TLongHashSet(Arrays.asList((long) edge.inVertex().id())));
        break;
      case OUT:
        edgeIds.put(Direction.OUT.name(), new TLongHashSet(Arrays.asList((long) edge.outVertex().id())));
        break;
      default: throw new NotImplementedException();
    }
    return edgeIds;
  }

}
