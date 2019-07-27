package io.shiftleft.overflowdb.structure;

import org.apache.tinkerpop.gremlin.structure.Vertex;

public final class TinkerHelper {

    private TinkerHelper() {
    }

//    protected static void addOutEdge(final TinkerVertex vertex, final String label, final Edge edge) {
//        if (null == vertex.outEdges) vertex.outEdges = new HashMap<>();
//        Set<Edge> edges = vertex.outEdges.get(label);
//        if (null == edges) {
//            edges = new HashSet<>();
//            vertex.outEdges.put(label, edges);
//        }
//        edges.add(edge);
//    }
//
//    protected static void addInEdge(final TinkerVertex vertex, final String label, final Edge edge) {
//        if (null == vertex.inEdges) vertex.inEdges = new HashMap<>();
//        Set<Edge> edges = vertex.inEdges.get(label);
//        if (null == edges) {
//            edges = new HashSet<>();
//            vertex.inEdges.put(label, edges);
//        }
//        edges.add(edge);
//    }
//
//    public static List<Vertex> queryVertexIndex(final TinkerGraph graph, final String key, final Object value) {
//        return null == graph.vertexIndex ? Collections.emptyList() : graph.vertexIndex.get(key, value);
//    }
//
//    public static List<Edge> queryEdgeIndex(final TinkerGraph graph, final String key, final Object value) {
//        return Collections.emptyList();
//    }
//
//    public static Map<String, List<VertexProperty>> getProperties(final TinkerVertex vertex) {
//        return null == vertex.properties ? Collections.emptyMap() : vertex.properties;
//    }

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
//
//    public static void removeIndex(final TinkerVertex vertex, final String key, final Object value) {
//        final TinkerGraph graph = (TinkerGraph) vertex.graph();
//        if (graph.vertexIndex != null)
//            graph.vertexIndex.remove(key, value, vertex);
//    }

//    public static void removeIndex(final TinkerEdge edge, final String key, final Object value) {
//        final TinkerGraph graph = (TinkerGraph) edge.graph();
//        if (graph.edgeIndex != null)
//            graph.edgeIndex.remove(key, value, edge);
//    }

}
