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

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.ElementRef;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.SpecializedElementFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.SpecializedTinkerVertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import java.util.Map;

public class VertexDeserializer extends Deserializer<Vertex> {
  protected final TinkerGraph graph;
  protected final Map<String, SpecializedElementFactory.ForVertex> vertexFactoryByLabel;

  public VertexDeserializer(TinkerGraph graph, Map<String, SpecializedElementFactory.ForVertex> vertexFactoryByLabel) {
    this.graph = graph;
    this.vertexFactoryByLabel = vertexFactoryByLabel;
  }

  @Override
  protected boolean elementRefRequiresAdjacentElements() {
    return false;
  }

  @Override
  protected ElementRef createElementRef(long id, String label, Map<String, long[]> inEdgeIdsByLabel, Map<String, long[]> outEdgeIdsByLabel) {
    SpecializedElementFactory.ForVertex vertexFactory = vertexFactoryByLabel.get(label);
    if (vertexFactory == null) {
      throw new AssertionError("vertexFactory not found for label=" + label);
    }

    return vertexFactory.createVertexRef(id, graph);
  }

  @Override
  protected Vertex createElement(long id, String label, Map<String, Object> properties, Map<String, long[]> inEdgeIdsByLabel, Map<String, long[]> outEdgeIdsByLabel) {
    SpecializedElementFactory.ForVertex vertexFactory = vertexFactoryByLabel.get(label);
    if (vertexFactory == null) {
      throw new AssertionError("vertexFactory not found for label=" + label);
    }
    SpecializedTinkerVertex vertex = vertexFactory.createVertex(id, graph);
    ElementHelper.attachProperties(vertex, VertexProperty.Cardinality.list, toTinkerpopKeyValues(properties));

    inEdgeIdsByLabel.entrySet().stream().forEach(entry -> {
      for (long edgeId : entry.getValue()) {
        vertex.storeInEdge(graph.edge(edgeId));
      }
    });

    outEdgeIdsByLabel.entrySet().stream().forEach(entry -> {
      for (long edgeId : entry.getValue()) {
        vertex.storeOutEdge(graph.edge(edgeId));
      }
    });

    vertex.setModifiedSinceLastSerialization(false);

    return vertex;
  }

}
