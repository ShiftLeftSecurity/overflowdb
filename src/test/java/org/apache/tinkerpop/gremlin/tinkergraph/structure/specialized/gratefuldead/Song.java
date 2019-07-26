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

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.*;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.Serializable;
import java.util.*;

public class Song extends SpecializedTinkerVertex implements Serializable {
    public static final String label = "song";

    public static final String NAME = "name";
    public static final String SONG_TYPE = "songType";
    public static final String PERFORMANCES = "performances";
    public static final String TEST_PROP = "testProperty";
    public static final Set<String> SPECIFIC_KEYS = new HashSet<>(Arrays.asList(NAME, SONG_TYPE, PERFORMANCES, TEST_PROP));
    public static final Set<String> ALLOWED_IN_EDGE_LABELS = new HashSet<>(Arrays.asList(FollowedBy.label));
    public static final Set<String> ALLOWED_OUT_EDGE_LABELS = new HashSet<>(Arrays.asList(FollowedBy.label, SungBy.label, WrittenBy.label));

    // properties
    private String name;
    private String songType;
    private Integer performances;
    private int[] testProp;

    public Song(Long id, TinkerGraph graph) {
        super(id, graph);
    }

    public List<FollowedBy> followedByIn() {
        return specializedEdges(Direction.IN, FollowedBy.label);
    }

    public List<FollowedBy> followedByOut() {
        return specializedEdges(Direction.OUT, FollowedBy.label);
    }
    
    public List<SungBy> sungByOut() {
        return specializedEdges(Direction.OUT, SungBy.label);
    }
    
    public List<WrittenBy> writtenByOut() {
        return specializedEdges(Direction.OUT, WrittenBy.label);
    }
    
    public String getName() {
        return name;
    }

    public String getSongType() {
        return songType;
    }

    public Integer getPerformances() {
        return performances;
    }

    public int[] getTestProp() { return testProp; }

    @Override
    protected Set<String> specificKeys() {
        return SPECIFIC_KEYS;
    }

    @Override
    public Set<String> allowedOutEdgeLabels() {
        return ALLOWED_OUT_EDGE_LABELS;
    }

    @Override
    public Set<String> allowedInEdgeLabels() {
        return ALLOWED_IN_EDGE_LABELS;
    }

    /* note: usage of `==` (pointer comparison) over `.equals` (String content comparison) is intentional for performance - use the statically defined strings */
    @Override
    protected <V> Iterator<VertexProperty<V>> specificProperties(String key) {
        final VertexProperty<V> ret;
        if (NAME.equals(key) && name != null) {
            return IteratorUtils.of(new SpecializedVertexProperty(this, key, name));
        } else if (key == SONG_TYPE && songType != null) {
            return IteratorUtils.of(new SpecializedVertexProperty(this, key, songType));
        } else if (key == PERFORMANCES && performances != null) {
            return IteratorUtils.of(new SpecializedVertexProperty(this, key, performances));
        } else if (key == TEST_PROP && testProp != null) {
            return IteratorUtils.of(new SpecializedVertexProperty(this, key, testProp));
        } else {
            return Collections.emptyIterator();
        }
    }

    @Override
    public Map<String, Object> valueMap() {
        Map<String, Object> properties = new HashMap<>();
        if (name != null) properties.put(NAME, name);
        if (songType != null) properties.put(SONG_TYPE, songType);
        if (performances != null) properties.put(PERFORMANCES, performances);
        if (testProp != null) properties.put(TEST_PROP, testProp);
        return properties;
    }

    @Override
    protected <V> VertexProperty<V> updateSpecificProperty(
      VertexProperty.Cardinality cardinality, String key, V value) {
        if (NAME.equals(key)) {
            this.name = (String) value;
        } else if (SONG_TYPE.equals(key)) {
            this.songType = (String) value;
        } else if (PERFORMANCES.equals(key)) {
            this.performances = ((Integer) value);
        } else if (TEST_PROP.equals(key)) {
            this.testProp = (int[]) value;
        } else {
            throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
        }
        return property(key);
    }


    @Override
    protected void removeSpecificProperty(String key) {
        if (NAME.equals(key)) {
            this.name = null;
        } else if (SONG_TYPE.equals(key)) {
            this.songType = null;
        } else if (PERFORMANCES.equals(key)) {
            this.performances = null;
        } else if (TEST_PROP.equals(key)) {
            this.testProp = null;
        } else {
            throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
        }
    }

    public static SpecializedElementFactory.ForVertex<Song> factory = new SpecializedElementFactory.ForVertex<Song>() {
        @Override
        public String forLabel() {
            return Song.label;
        }

        @Override
        public Song createVertex(Long id, TinkerGraph graph) {
            return new Song(id, graph);
        }

        @Override
        public VertexRef<Song> createVertexRef(Song vertex) {
            return new VertexRefWithLabel<>(vertex.id(), vertex.graph(), vertex, Song.label);
        }

        @Override
        public VertexRef<Song> createVertexRef(Long id, TinkerGraph graph) {
            return new VertexRefWithLabel<>(id, graph, null, Song.label);
        }
    };

    @Override
    public String label() {
        return Song.label;
    }
}
