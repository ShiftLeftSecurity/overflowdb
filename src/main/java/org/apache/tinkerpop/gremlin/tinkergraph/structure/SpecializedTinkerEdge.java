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
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;
import java.util.concurrent.Semaphore;

import static org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerElement.elementAlreadyRemoved;

public abstract class SpecializedTinkerEdge implements Edge {
    private final long id;
    private final TinkerGraph graph;
    private final Vertex inVertex;
    private final Vertex outVertex;
    private final String label;
    private boolean removed = false;

    /** `dirty` flag for serialization to avoid superfluous serialization */
    // TODO re-implement/verify this optimization: only re-serialize if element has been changed
    private boolean modifiedSinceLastSerialization = true;

    private final Set<String> specificKeys;

    protected SpecializedTinkerEdge(TinkerGraph graph, Long id, Vertex outVertex, String label, Vertex inVertex, Set<String> specificKeys) {
        this.id = id;
        this.graph = graph;
        this.inVertex = inVertex;
        this.outVertex = outVertex;
        this.label = label;

        this.specificKeys = specificKeys;
        if (graph.referenceManager != null) {
            graph.referenceManager.applyBackpressureMaybe();
        }
    }

    @Override
    public Object id() {
        return id;
    }

    @Override
    public String label() {
        return label;
    }

    @Override
    public Set<String> keys() {
        return specificKeys;
    }

    @Override
    public <V> Property<V> property(String key) {
        return specificProperty(key);
    }

    /* implement in concrete specialised instance to avoid using generic HashMaps */
    protected abstract <V> Property<V> specificProperty(String key);

    @Override
    public <V> Iterator<Property<V>> properties(String... propertyKeys) {
        if (propertyKeys.length == 0) {
            return (Iterator) specificKeys.stream().map(key -> property(key)).filter(vp -> vp.isPresent()).iterator();
        } else if (propertyKeys.length == 1) { // treating as special case for performance
            // return IteratorUtils.of(property(propertyKeys[0]));
            final Property<V> prop = property(propertyKeys[0]);
            return prop.isPresent() ? IteratorUtils.of(prop) : Collections.emptyIterator();
        } else {
            return Arrays.stream(propertyKeys).map(key -> (Property<V>) property(key)).filter(vp -> vp.isPresent()).iterator();
        }
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        if (this.removed) throw elementAlreadyRemoved(Edge.class, id);
        ElementHelper.validateProperty(key, value);
        synchronized (this) {
            modifiedSinceLastSerialization = true;
            final Property<V> p = updateSpecificProperty(key, value);
            TinkerHelper.autoUpdateIndex(this, key, value, null);
            return p;
        }
    }

    protected abstract <V> Property<V> updateSpecificProperty(String key, V value);

    public void removeProperty(String key) {
        synchronized (this) {
            modifiedSinceLastSerialization = true;
            removeSpecificProperty(key);
        }
    }

    protected abstract void removeSpecificProperty(String key);

    @Override
    public Graph graph() {
        return this.graph;
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction) {
        if (removed) return Collections.emptyIterator();
        switch (direction) {
            case OUT:
                return IteratorUtils.of(outVertex);
            case IN:
                return IteratorUtils.of(inVertex);
            default:
                return IteratorUtils.of(outVertex, inVertex);
        }
    }

    @Override
    public void remove() {
        final SpecializedTinkerVertex outVertex;
        final SpecializedTinkerVertex inVertex;
        if (this.outVertex instanceof ElementRef) {
            outVertex = ((ElementRef<SpecializedTinkerVertex>) this.outVertex).get();
        } else {
            outVertex = (SpecializedTinkerVertex) this.outVertex;
        }
        if (this.inVertex instanceof ElementRef) {
            inVertex = ((ElementRef<SpecializedTinkerVertex>) this.inVertex).get();
        } else {
            inVertex = (SpecializedTinkerVertex) this.inVertex;
        }

        if (null != outVertex && null != outVertex.outEdgesByLabel) {
            final List<Edge> edges = outVertex.outEdgesByLabel.get(label());
            if (edges != null) {
                edges.remove(this);
            }
        }
        if (null != inVertex && null != inVertex.inEdgesByLabel) {
            final List<Edge> edges = inVertex.inEdgesByLabel.get(label());
            if (null != edges)
                edges.remove(this);
        }

        TinkerHelper.removeElementIndex(this);
        graph.edges.remove(id);
        graph.getElementsByLabel(graph.edgesByLabel, label).remove(this);
        if (graph.ondiskOverflowEnabled) {
            graph.ondiskOverflow.removeEdge((Long) id);
        }

        this.removed = true;

        modifiedSinceLastSerialization = true;
    }

    public boolean isRemoved() {
        return removed;
    }

    public void setModifiedSinceLastSerialization(boolean modifiedSinceLastSerialization) {
      this.modifiedSinceLastSerialization = modifiedSinceLastSerialization;
    }

    public boolean isModifiedSinceLastSerialization() {
      return modifiedSinceLastSerialization;
    }

}
