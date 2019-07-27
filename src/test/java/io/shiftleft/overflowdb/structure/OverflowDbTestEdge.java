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
package io.shiftleft.overflowdb.structure;

import java.util.Arrays;
import java.util.HashSet;

public class OverflowDbTestEdge extends OverflowDbEdge {
    public static final String LABEL = "testEdge";
    public static final String LONG_PROPERTY = "longProperty";
    public static final HashSet<String> PROPERTY_KEYS = new HashSet<>(Arrays.asList(LONG_PROPERTY));

    public OverflowDbTestEdge(TinkerGraph graph, VertexRef<OverflowDbNode> outVertex, VertexRef<OverflowDbNode> inVertex) {
        super(graph, LABEL, outVertex, inVertex, PROPERTY_KEYS);
    }

    public Long longProperty() {
        return (Long) property(LONG_PROPERTY).value();
    }

    public static final EdgeLayoutInformation layoutInformation = new EdgeLayoutInformation(LABEL, PROPERTY_KEYS);

    public static OverflowElementFactory.ForEdge<OverflowDbTestEdge> factory = new OverflowElementFactory.ForEdge<OverflowDbTestEdge>() {
        @Override
        public String forLabel() {
            return OverflowDbTestEdge.LABEL;
        }

        @Override
        public OverflowDbTestEdge createEdge(TinkerGraph graph, VertexRef outVertex, VertexRef inVertex) {
            return new OverflowDbTestEdge(graph, outVertex, inVertex);
        }
    };
}
