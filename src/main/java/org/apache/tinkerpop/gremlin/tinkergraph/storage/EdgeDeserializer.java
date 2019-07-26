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

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.ElementRef;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.SpecializedElementFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.SpecializedTinkerEdge;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.VertexRef;

import java.util.Map;

public class EdgeDeserializer extends Deserializer<Edge> {
  protected final TinkerGraph graph;
  protected final Map<String, SpecializedElementFactory.ForEdge> edgeFactoryByLabel;

  public EdgeDeserializer(TinkerGraph graph, Map<String, SpecializedElementFactory.ForEdge> edgeFactoryByLabel) {
    this.graph = graph;
    this.edgeFactoryByLabel = edgeFactoryByLabel;
  }

  @Override
  protected boolean elementRefRequiresAdjacentElements() {
    return true;
  }

  @Override
  protected ElementRef createElementRef(long id, String label, Map<String, long[]> inVertexIdsByLabel, Map<String, long[]> outVertexIdsByLabel) {
    VertexRef outVertexRef = getVertexRef(outVertexIdsByLabel, Direction.OUT);
    VertexRef inVertexRef = getVertexRef(inVertexIdsByLabel, Direction.IN);
    return edgeFactoryByLabel.get(label).createEdgeRef(id, graph, outVertexRef, inVertexRef);
  }

  @Override
  protected Edge createElement(long id, String label, Map<String, Object> properties, Map<String, long[]> inVertexIdsByLabel, Map<String, long[]> outVertexIdsByLabel) {
    VertexRef outVertexRef = getVertexRef(outVertexIdsByLabel, Direction.OUT);
    VertexRef inVertexRef = getVertexRef(inVertexIdsByLabel, Direction.IN);
    SpecializedTinkerEdge edge = edgeFactoryByLabel.get(label).createEdge(id, graph, outVertexRef, inVertexRef);
    ElementHelper.attachProperties(edge, toTinkerpopKeyValues(properties));

    edge.setModifiedSinceLastSerialization(false);
    return edge;
  }

  private VertexRef getVertexRef(Map<String, long[]> vertexIdsByLabel, Direction direction) {
    final long[] vertexIds = vertexIdsByLabel.get(direction.name());
    assert vertexIds != null;
    assert vertexIds.length == 1;
    return (VertexRef) graph.vertex(vertexIds[0]);
  }

}
