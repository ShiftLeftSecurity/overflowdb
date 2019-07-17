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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class OverflowDbTestEdge extends OverflowDbEdge {
    public static final String label = "testEdge";

    public static final String LONG_PROPERTY = "longProperty";
    public static final Set<String> SPECIFIC_KEYS = new HashSet<>(Arrays.asList(LONG_PROPERTY));

    public OverflowDbTestEdge(TinkerGraph graph, VertexRef<OverflowDbNode> outVertex, VertexRef<OverflowDbNode> inVertex) {
        super(graph, label, outVertex, inVertex, SPECIFIC_KEYS);
    }

    public Long longProperty() {
        return (Long) property(LONG_PROPERTY).value();
    }

    public static OverflowElementFactory.ForEdge<OverflowDbTestEdge> factory = new OverflowElementFactory.ForEdge<OverflowDbTestEdge>() {
        @Override
        public String forLabel() {
            return OverflowDbTestEdge.label;
        }

        @Override
        public OverflowDbTestEdge createEdge(Long id, TinkerGraph graph, VertexRef outVertex, VertexRef inVertex) {
            return new OverflowDbTestEdge(graph, outVertex, inVertex);
        }
    };
}
