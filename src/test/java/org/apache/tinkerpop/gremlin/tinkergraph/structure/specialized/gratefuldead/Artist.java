///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations
// * under the License.
// */
//package org.apache.tinkerpop.gremlin.tinkergraph.structure.specialized.gratefuldead;
//
//import org.apache.tinkerpop.gremlin.structure.Direction;
//import org.apache.tinkerpop.gremlin.structure.VertexProperty;
//import org.apache.tinkerpop.gremlin.tinkergraph.structure.*;
//import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
//import java.util.*;
//
//public class Artist extends SpecializedTinkerVertex {
//    public static final String label = "artist";
//
//    public static final String NAME = "name";
//    public static final Set<String> SPECIFIC_KEYS = new HashSet<>(Arrays.asList(NAME));
//    public static final Set<String> ALLOWED_IN_EDGE_LABELS = new HashSet<>(Arrays.asList(SungBy.label, WrittenBy.label));
//    public static final Set<String> ALLOWED_OUT_EDGE_LABELS = new HashSet<>();
//
//    // properties
//    private String name;
//
//    public Artist(VertexRef ref) {
//        super(ref);
//    }
//
//    public String getName() {
//        return name;
//    }
//
//    public List<SungBy> sungByIn() {
//        return specializedEdges(Direction.IN, SungBy.label);
//    }
//
//    public List<WrittenBy> writtenByIn() {
//        return specializedEdges(Direction.IN, WrittenBy.label);
//    }
//
//
//    @Override
//    protected Set<String> specificKeys() {
//        return SPECIFIC_KEYS;
//    }
//
//    @Override
//    public Set<String> allowedOutEdgeLabels() {
//        return ALLOWED_OUT_EDGE_LABELS;
//    }
//
//    @Override
//    public Set<String> allowedInEdgeLabels() {
//        return ALLOWED_IN_EDGE_LABELS;
//    }
//
//    /* note: usage of `==` (pointer comparison) over `.equals` (String content comparison) is intentional for performance - use the statically defined strings */
//    @Override
//    protected <V> Iterator<VertexProperty<V>> specificProperties(String key) {
//        final VertexProperty<V> ret;
//        if (NAME.equals(key) && name != null) {
//            return IteratorUtils.of(new OverflowNodeProperty(this, key, name));
//        } else {
//            return Collections.emptyIterator();
//        }
//    }
//
//    @Override
//    public Map<String, Object> valueMap() {
//        Map<String, Object> properties = new HashMap<>();
//        if (name != null) properties.put(NAME, name);
//        return properties;
//    }
//
//    @Override
//    protected <V> VertexProperty<V> updateSpecificProperty(
//      VertexProperty.Cardinality cardinality, String key, V value) {
//        if (NAME.equals(key)) {
//            this.name = (String) value;
//        } else {
//            throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
//        }
//        return property(key);
//    }
//
//    @Override
//    protected void removeSpecificProperty(String key) {
//        if (NAME.equals(key)) {
//            this.name = null;
//        } else {
//            throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
//        }
//    }
//
//    public static SpecializedElementFactory.ForVertex<Artist> factory = new SpecializedElementFactory.ForVertex<Artist>() {
//        @Override
//        public String forLabel() {
//            return Artist.label;
//        }
//
//        @Override
//        public Artist createVertex(Long id, TinkerGraph graph) {
//            return new Artist(createVertexRef(id, graph));
//        }
//
//        @Override
//        public Artist createVertex(VertexRef<Artist> ref) {
//            return new Artist(ref);
//        }
//
//        @Override
//        public VertexRef<Artist> createVertexRef(Long id, TinkerGraph graph) {
//            return new VertexRefWithLabel<>(id, graph, null, Artist.label);
//        }
//    };
//
//    @Override
//    public String label() {
//        return Artist.label;
//    }
//}
