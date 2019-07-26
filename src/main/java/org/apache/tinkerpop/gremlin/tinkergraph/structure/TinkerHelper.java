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

import gnu.trove.map.TLongObjectMap;
import org.apache.tinkerpop.gremlin.process.computer.GraphFilter;
import org.apache.tinkerpop.gremlin.process.computer.VertexComputeKey;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.tinkergraph.process.computer.TinkerGraphComputerView;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TinkerHelper {

    private TinkerHelper() {
    }

//    protected static Edge addEdge(final TinkerGraph graph, final TinkerVertex outVertex, final TinkerVertex inVertex, final String label, final Object... keyValues) {
//        ElementHelper.validateLabel(label);
//        ElementHelper.legalPropertyKeyValueArray(keyValues);
//
//        Object idValue = graph.edgeIdManager.convert(ElementHelper.getIdValue(keyValues).orElse(null));
//
//        final Edge edge;
//        if (null != idValue) {
//            if (graph.edges.containsKey((long)idValue))
//                throw Graph.Exceptions.edgeWithIdAlreadyExists(idValue);
//        } else {
//            idValue = graph.edgeIdManager.getNextId(graph);
//        }
//
//        edge = new TinkerEdge(graph, idValue, outVertex, label, inVertex);
//        ElementHelper.attachProperties(edge, keyValues);
//        TinkerHelper.addOutEdge(outVertex, label, edge);
//        TinkerHelper.addInEdge(inVertex, label, edge);
//        return edge;
//
//    }

    protected static void addOutEdge(final TinkerVertex vertex, final String label, final Edge edge) {
        if (null == vertex.outEdges) vertex.outEdges = new HashMap<>();
        Set<Edge> edges = vertex.outEdges.get(label);
        if (null == edges) {
            edges = new HashSet<>();
            vertex.outEdges.put(label, edges);
        }
        edges.add(edge);
    }

    protected static void addInEdge(final TinkerVertex vertex, final String label, final Edge edge) {
        if (null == vertex.inEdges) vertex.inEdges = new HashMap<>();
        Set<Edge> edges = vertex.inEdges.get(label);
        if (null == edges) {
            edges = new HashSet<>();
            vertex.inEdges.put(label, edges);
        }
        edges.add(edge);
    }

    public static List<Vertex> queryVertexIndex(final TinkerGraph graph, final String key, final Object value) {
        return null == graph.vertexIndex ? Collections.emptyList() : graph.vertexIndex.get(key, value);
    }

    public static List<Edge> queryEdgeIndex(final TinkerGraph graph, final String key, final Object value) {
        return Collections.emptyList();
    }

    public static boolean inComputerMode(final TinkerGraph graph) {
        return null != graph.graphComputerView;
    }

    public static TinkerGraphComputerView createGraphComputerView(final TinkerGraph graph, final GraphFilter graphFilter, final Set<VertexComputeKey> computeKeys) {
        return graph.graphComputerView = new TinkerGraphComputerView(graph, graphFilter, computeKeys);
    }

    public static TinkerGraphComputerView getGraphComputerView(final TinkerGraph graph) {
        return graph.graphComputerView;
    }

    public static void dropGraphComputerView(final TinkerGraph graph) {
        graph.graphComputerView = null;
    }

    public static Map<String, List<VertexProperty>> getProperties(final TinkerVertex vertex) {
        return null == vertex.properties ? Collections.emptyMap() : vertex.properties;
    }

//    public static void autoUpdateIndex(final Edge edge, final String key, final Object newValue, final Object oldValue) {
//        final TinkerGraph graph = (TinkerGraph) edge.graph();
//
//        if (graph.edgeIndex != null)
//            graph.edgeIndex.autoUpdate(key, newValue, oldValue, edge);
//    }

    public static void autoUpdateIndex(final Vertex vertex, final String key, final Object newValue, final Object oldValue) {
        final TinkerGraph graph = (TinkerGraph) vertex.graph();
        if (graph.vertexIndex != null)
            graph.vertexIndex.autoUpdate(key, newValue, oldValue, vertex);
    }

    public static void removeElementIndex(final Vertex vertex) {
        final TinkerGraph graph = (TinkerGraph) vertex.graph();
        if (graph.vertexIndex != null)
            graph.vertexIndex.removeElement(vertex);
    }

//    public static void removeElementIndex(final Edge edge) {
//        final TinkerGraph graph = (TinkerGraph) edge.graph();
//        if (graph.edgeIndex != null)
//            graph.edgeIndex.removeElement(edge);
//    }

    public static void removeIndex(final TinkerVertex vertex, final String key, final Object value) {
        final TinkerGraph graph = (TinkerGraph) vertex.graph();
        if (graph.vertexIndex != null)
            graph.vertexIndex.remove(key, value, vertex);
    }

//    public static void removeIndex(final TinkerEdge edge, final String key, final Object value) {
//        final TinkerGraph graph = (TinkerGraph) edge.graph();
//        if (graph.edgeIndex != null)
//            graph.edgeIndex.remove(key, value, edge);
//    }

    public static Iterator<TinkerEdge> getEdges(final TinkerVertex vertex, final Direction direction, final String... edgeLabels) {
        final List<Edge> edges = new ArrayList<>();
        if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
            if (vertex.outEdges != null) {
                if (edgeLabels.length == 0)
                    vertex.outEdges.values().forEach(edges::addAll);
                else if (edgeLabels.length == 1)
                    edges.addAll(vertex.outEdges.getOrDefault(edgeLabels[0], Collections.emptySet()));
                else
                    Stream.of(edgeLabels).map(vertex.outEdges::get).filter(Objects::nonNull).forEach(edges::addAll);
            }
        }
        if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
            if (vertex.inEdges != null) {
                if (edgeLabels.length == 0)
                    vertex.inEdges.values().forEach(edges::addAll);
                else if (edgeLabels.length == 1)
                    edges.addAll(vertex.inEdges.getOrDefault(edgeLabels[0], Collections.emptySet()));
                else
                    Stream.of(edgeLabels).map(vertex.inEdges::get).filter(Objects::nonNull).forEach(edges::addAll);
            }
        }
        return (Iterator) edges.iterator();
    }

    public static Iterator<TinkerVertex> getVertices(final TinkerVertex vertex, final Direction direction, final String... edgeLabels) {
        final List<Vertex> vertices = new ArrayList<>();
        if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
            if (vertex.outEdges != null) {
                if (edgeLabels.length == 0)
                    vertex.outEdges.values().forEach(set -> set.forEach(edge -> vertices.add(((TinkerEdge) edge).inVertex)));
                else if (edgeLabels.length == 1)
                    vertex.outEdges.getOrDefault(edgeLabels[0], Collections.emptySet()).forEach(edge -> vertices.add(((TinkerEdge) edge).inVertex));
                else
                    Stream.of(edgeLabels).map(vertex.outEdges::get).filter(Objects::nonNull).flatMap(Set::stream).forEach(edge -> vertices.add(((TinkerEdge) edge).inVertex));
            }
        }
        if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
            if (vertex.inEdges != null) {
                if (edgeLabels.length == 0)
                    vertex.inEdges.values().forEach(set -> set.forEach(edge -> vertices.add(((TinkerEdge) edge).outVertex)));
                else if (edgeLabels.length == 1)
                    vertex.inEdges.getOrDefault(edgeLabels[0], Collections.emptySet()).forEach(edge -> vertices.add(((TinkerEdge) edge).outVertex));
                else
                    Stream.of(edgeLabels).map(vertex.inEdges::get).filter(Objects::nonNull).flatMap(Set::stream).forEach(edge -> vertices.add(((TinkerEdge) edge).outVertex));
            }
        }
        return (Iterator) vertices.iterator();
    }

    public static TLongObjectMap<Vertex> getVertices(final TinkerGraph graph) {
        return graph.vertices;
    }
}
