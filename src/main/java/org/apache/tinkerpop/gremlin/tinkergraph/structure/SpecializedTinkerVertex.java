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

import gnu.trove.map.hash.THashMap;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.tinkerpop.gremlin.util.iterator.MultiIterator;

import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerElement.elementAlreadyRemoved;

public abstract class SpecializedTinkerVertex implements Vertex {

    private final long id;
    private final TinkerGraph graph;
    private boolean removed = false;

    /** property keys for a specialized vertex  */
    protected abstract Set<String> specificKeys();

    public abstract Set<String> allowedOutEdgeLabels();
    public abstract Set<String> allowedInEdgeLabels();

    protected Map<String, List<Edge>> outEdgesByLabel;
    protected Map<String, List<Edge>> inEdgesByLabel;

    /** `dirty` flag for serialization to avoid superfluous serialization */
    // TODO re-implement/verify this optimization: only re-serialize if element has been changed
    private boolean modifiedSinceLastSerialization = true;

    protected SpecializedTinkerVertex(long id, TinkerGraph graph) {
        this.id = id;
        this.graph = graph;

        if (graph != null && graph.referenceManager != null) {
            graph.referenceManager.applyBackpressureMaybe();
        }
    }

    @Override
    public Graph graph() {
        return graph;
    }

    @Override
    public Object id() {
        return id;
    }

    @Override
    public Set<String> keys() {
        return specificKeys();
    }

    @Override
    public <V> VertexProperty<V> property(String key) {
        if (this.removed) return VertexProperty.empty();
        return specificProperty(key);
    }

    /* You can override this default implementation in concrete specialised instances for performance
     * if you like, since technically the Iterator isn't necessary.
     * This default implementation works fine though. */
    protected <V> VertexProperty<V> specificProperty(String key) {
        Iterator<VertexProperty<V>> iter = specificProperties(key);
        if (iter.hasNext()) {
          return iter.next();
        } else {
          return VertexProperty.empty();
        }
    }

    /* implement in concrete specialised instance to avoid using generic HashMaps */
    protected abstract <V> Iterator<VertexProperty<V>> specificProperties(String key);

    public abstract Map<String, Object> valueMap();

    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
        if (this.removed) return Collections.emptyIterator();
        if (propertyKeys.length == 0) { // return all properties
            return (Iterator) specificKeys().stream().flatMap(key ->
                StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                  specificProperties(key), Spliterator.ORDERED),false)
            ).iterator();
        } else if (propertyKeys.length == 1) { // treating as special case for performance
            return specificProperties(propertyKeys[0]);
        } else {
            return (Iterator) Arrays.stream(propertyKeys).flatMap(key ->
              StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                specificProperties(key), Spliterator.ORDERED),false)
            ).iterator();
        }
    }

    @Override
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... keyValues) {
        if (this.removed) throw elementAlreadyRemoved(Vertex.class, id);
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        ElementHelper.validateProperty(key, value);
        synchronized (this) {
            this.modifiedSinceLastSerialization = true;
            final VertexProperty<V> vp = updateSpecificProperty(cardinality, key, value);
            TinkerHelper.autoUpdateIndex(this, key, value, null);
            return vp;
        }
    }

    protected abstract <V> VertexProperty<V> updateSpecificProperty(
      VertexProperty.Cardinality cardinality, String key, V value);

    public void removeProperty(String key) {
        synchronized (this) {
            modifiedSinceLastSerialization = true;
            removeSpecificProperty(key);
        }
    }

    protected abstract void removeSpecificProperty(String key);

    @Override
    public Edge addEdge(final String label, Vertex inVertex, final Object... keyValues) {
        if (graph.isClosed()) {
            throw new IllegalStateException("cannot add more elements, graph is closed");
        }
        if (null == inVertex) {
            throw Graph.Exceptions.argumentCanNotBeNull("inVertex");
        }
        VertexRef<TinkerVertex> inVertexRef = null;
        if (inVertex instanceof VertexRef) {
            inVertexRef = (VertexRef) inVertex;
            inVertex = inVertexRef.get();
        }
        if (this.removed) {
            throw elementAlreadyRemoved(Vertex.class, this.id);
        }
        if (!allowedOutEdgeLabels().contains(label)) {
            throw new IllegalArgumentException(getClass().getName() + " doesn't allow outgoing edges with label=" + label);
        }
        if (!((SpecializedTinkerVertex) inVertex).allowedInEdgeLabels().contains(label)) {
            throw new IllegalArgumentException(inVertex.getClass().getName() + " doesn't allow incoming edges with label=" + label);
        }
        ElementHelper.legalPropertyKeyValueArray(keyValues);

        if (graph.specializedEdgeFactoryByLabel.containsKey(label)) {
            SpecializedElementFactory.ForEdge factory = graph.specializedEdgeFactoryByLabel.get(label);
            Long idValue = (Long) graph.edgeIdManager.convert(ElementHelper.getIdValue(keyValues).orElse(null));
            if (null != idValue) {
                if (graph.edges.containsKey(idValue))
                    throw Graph.Exceptions.edgeWithIdAlreadyExists(idValue);
            } else {
                idValue = (Long) graph.edgeIdManager.getNextId(graph);
            }
            graph.currentId.set(Long.max(idValue, graph.currentId.get()));

            // TODO hold link to vertexRef locally so we don't need the following lookup
            VertexRef<TinkerVertex> outVertexRef = (VertexRef<TinkerVertex>) graph.vertices.get(id);
            final SpecializedTinkerEdge underlying = factory.createEdge(idValue, graph, outVertexRef, inVertexRef);
            final Edge edge;
            if (graph.ondiskOverflowEnabled) {
                edge = factory.createEdgeRef(underlying);
            } else {
                edge = factory.createEdge(idValue, graph, outVertexRef, inVertexRef);
            }
            ElementHelper.attachProperties(edge, keyValues);
            graph.edges.put((long)edge.id(), edge);
            graph.getElementsByLabel(graph.edgesByLabel, label).add(edge);

//            acquireModificationLock();
            storeOutEdge(edge);
            ((SpecializedTinkerVertex) inVertex).storeInEdge(edge);
//            releaseModificationLock();
            modifiedSinceLastSerialization = true;
            return edge;
        } else { // edge label not registered for a specialized factory, treating as generic edge
            throw new IllegalArgumentException(
                "this instance of TinkerGraph uses specialized elements, but doesn't have a factory for label " + label
                    + ". Mixing specialized and generic elements is not (yet) supported");
        }
    }

    /** do not call directly (other than from deserializer and SpecializedTinkerVertex.addEdge) */
    public void storeOutEdge(final Edge edge) {
        storeEdge(edge, getOutEdgesByLabel());
    }
    
    /** do not call directly (other than from deserializer and SpecializedTinkerVertex.addEdge) */
    public void storeInEdge(final Edge edge) {
        storeEdge(edge, getInEdgesByLabel());
    }

    private void storeEdge(final Edge edge, final Map<String, List<Edge>> edgesByLabel) {
        if (!edgesByLabel.containsKey(edge.label())) {
            // TODO ArrayLists aren't good for concurrent modification, use memory-light concurrency safe list
            edgesByLabel.put(edge.label(), new ArrayList<>());
        }
        edgesByLabel.get(edge.label()).add(edge);
    }

    @Override
    public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
        final MultiIterator<Edge> multiIterator = new MultiIterator<>();

        if (edgeLabels.length == 0) { // follow all labels
            if (direction == Direction.OUT || direction == Direction.BOTH) {
                getOutEdgesByLabel().values().forEach(edges -> multiIterator.addIterator(edges.iterator()));
            }
            if (direction == Direction.IN || direction == Direction.BOTH) {
                getInEdgesByLabel().values().forEach(edges -> multiIterator.addIterator(edges.iterator()));
            }
        } else {
            for (String label : edgeLabels) {
                /* note: usage of `==` (pointer comparison) over `.equals` (String content comparison) is intentional for performance - use the statically defined strings */
                if (direction == Direction.OUT || direction == Direction.BOTH) {
                    multiIterator.addIterator(getOutEdgesByLabel(label).iterator());
                }
                if (direction == Direction.IN || direction == Direction.BOTH) {
                    multiIterator.addIterator(getInEdgesByLabel(label).iterator());
                }
            }
        }
        
        return multiIterator;
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
        Iterator<Edge> edges = edges(direction, edgeLabels);
        if (direction == Direction.IN) {
            return IteratorUtils.map(edges, Edge::outVertex);
        } else if (direction == Direction.OUT) {
            return IteratorUtils.map(edges, Edge::inVertex);
        } else if (direction == Direction.BOTH) {
            return IteratorUtils.concat(vertices(Direction.IN, edgeLabels), vertices(Direction.OUT, edgeLabels));
        } else {
            return Collections.emptyIterator();
        }
    }

    protected Map<String, List<Edge>> getOutEdgesByLabel() {
        if (outEdgesByLabel == null) {
            this.outEdgesByLabel = new THashMap<>();
        }
        return outEdgesByLabel;
    }
    
    protected Map<String, List<Edge>> getInEdgesByLabel() {
        if (inEdgesByLabel == null) {
            this.inEdgesByLabel = new THashMap<>();
        }
        return inEdgesByLabel;
    }

    protected List<Edge> getOutEdgesByLabel(String label) {
        return getOutEdgesByLabel().getOrDefault(label, new ArrayList<>());
    }

    protected List<Edge> getInEdgesByLabel(String label) {
        return getInEdgesByLabel().getOrDefault(label, new ArrayList<>());
    }

    protected <E extends SpecializedTinkerEdge> List<E> specializedEdges(final Direction direction, final String label) {
        final Map<String, List<Edge>> edgesByLabel;
        if (direction == Direction.OUT) {
            edgesByLabel = getOutEdgesByLabel();
        } else if (direction == Direction.IN) {
            edgesByLabel = getInEdgesByLabel();
        } else {
            throw new IllegalArgumentException("not implemented");
        }

        return edgesByLabel.get(label).stream().map(edge -> {
            if (edge instanceof EdgeRef) {
                edge = ((EdgeRef<E>) edge).get();
            }
            return (E) edge;
        }).collect(Collectors.toList());
    }

    @Override
    public void remove() {
        final List<Edge> edges = new ArrayList<>();
        this.edges(Direction.BOTH).forEachRemaining(edges::add);
        edges.stream().filter(edge -> {
            if (edge instanceof ElementRef) {
                return !((ElementRef<SpecializedTinkerEdge>) edge).isRemoved();
            } else {
                return !((SpecializedTinkerEdge) edge).isRemoved();
            }
        }).forEach(Edge::remove);
        TinkerHelper.removeElementIndex(this);
        graph.vertices.remove((long)id());
        graph.getElementsByLabel(graph.verticesByLabel, label()).remove(this);

        if (graph.ondiskOverflowEnabled) {
            graph.ondiskOverflow.removeVertex((Long) id);
        }
        this.removed = true;
        this.modifiedSinceLastSerialization = true;
    }

    public void setModifiedSinceLastSerialization(boolean modifiedSinceLastSerialization) {
        this.modifiedSinceLastSerialization = modifiedSinceLastSerialization;
    }
}
