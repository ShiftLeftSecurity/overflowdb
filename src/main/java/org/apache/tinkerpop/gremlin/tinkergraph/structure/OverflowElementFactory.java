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

/* To make use of specialized elements (for better memory/performance characteristics), you need to
 * create instances of these factories and register them with TinkerGraph. That way it will instantiate
 * your specialized elements rather than generic ones. */
public class OverflowElementFactory {
    public interface ForVertex<V extends OverflowDbNode> {
        String forLabel();
        V createVertex(Long id, TinkerGraph graph);
        V createVertex(VertexRef<V> ref);
        VertexRef<V> createVertexRef(Long id, TinkerGraph graph);
    }

    public interface ForEdge<E extends OverflowDbEdge> {
        String forLabel();
        E createEdge(TinkerGraph graph, VertexRef outVertex, VertexRef inVertex);
    }
}

