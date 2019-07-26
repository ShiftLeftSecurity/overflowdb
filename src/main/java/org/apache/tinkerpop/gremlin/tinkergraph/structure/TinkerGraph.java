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
import gnu.trove.map.hash.THashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.THashSet;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoVersion;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.strategy.optimization.TinkerGraphCountStrategy;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.strategy.optimization.TinkerGraphStepStrategy;
import org.apache.tinkerpop.gremlin.tinkergraph.storage.EdgeDeserializer;
import org.apache.tinkerpop.gremlin.tinkergraph.storage.OndiskOverflow;
import org.apache.tinkerpop.gremlin.tinkergraph.storage.VertexDeserializer;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.tinkerpop.gremlin.util.iterator.MultiIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

public final class TinkerGraph implements Graph {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    static {
        TraversalStrategies.GlobalCache.registerStrategies(TinkerGraph.class, TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone().addStrategies(
                TinkerGraphStepStrategy.instance(),
                TinkerGraphCountStrategy.instance()));
    }

    public static final Configuration EMPTY_CONFIGURATION() {
        return new BaseConfiguration() {{
            this.setProperty(Graph.GRAPH, TinkerGraph.class.getName());
            this.setProperty(GREMLIN_TINKERGRAPH_OVERFLOW_HEAP_PERCENTAGE_THRESHOLD, 80);
            this.setProperty(GREMLIN_TINKERGRAPH_ONDISK_OVERFLOW_ENABLED, true);
        }};
    }

    public static final String GREMLIN_TINKERGRAPH_VERTEX_ID_MANAGER = "gremlin.tinkergraph.vertexIdManager";
    public static final String GREMLIN_TINKERGRAPH_EDGE_ID_MANAGER = "gremlin.tinkergraph.edgeIdManager";
    public static final String GREMLIN_TINKERGRAPH_VERTEX_PROPERTY_ID_MANAGER = "gremlin.tinkergraph.vertexPropertyIdManager";
    public static final String GREMLIN_TINKERGRAPH_DEFAULT_VERTEX_PROPERTY_CARDINALITY = "gremlin.tinkergraph.defaultVertexPropertyCardinality";
    public static final String GREMLIN_TINKERGRAPH_GRAPH_LOCATION = "gremlin.tinkergraph.graphLocation";
    public static final String GREMLIN_TINKERGRAPH_GRAPH_FORMAT = "gremlin.tinkergraph.graphFormat";
    public static final String GREMLIN_TINKERGRAPH_ONDISK_OVERFLOW_ENABLED = "gremlin.tinkergraph.ondiskOverflow.enabled";

    /** when heap (after GC run) is above this threshold (e.g. 80 for 80%), @see ReferenceManager will start to clear some references, i.e. write them to storage and set them to `null` */
    public static final String GREMLIN_TINKERGRAPH_OVERFLOW_HEAP_PERCENTAGE_THRESHOLD = "gremlin.tinkergraph.ondiskOverflow.heapPercentageThreshold";

    public static final String GRAPH_FORMAT_MVSTORE = "overflowdb.graphFormat.mvstore";

    private final TinkerGraphFeatures features = new TinkerGraphFeatures();

    protected AtomicLong currentId = new AtomicLong(-1L);
    // TODO: replace with the more memory efficient `TLongHashMap`
    // note: if on-disk overflow enabled, these [Vertex|Edge] values are [VertexRef|ElementRef]
    protected TLongObjectMap<Vertex> vertices;
    protected TLongObjectMap<Edge> edges;
    protected THashMap<String, Set<Vertex>> verticesByLabel;
    protected THashMap<String, Set<Edge>> edgesByLabel;

    protected TinkerGraphVariables variables = null;
    protected TinkerIndex<Vertex> vertexIndex = null;
    protected TinkerIndex<Edge> edgeIndex = null;
    protected final IdManager vertexIdManager;

    protected final boolean usesSpecializedElements;
    protected final Map<String, SpecializedElementFactory.ForVertex> specializedVertexFactoryByLabel;
    protected final Map<String, SpecializedElementFactory.ForEdge> specializedEdgeFactoryByLabel;

    private final Configuration configuration;
    private final String graphLocation;
    private final String graphFormat;
    private boolean closed = false;

    /* overflow to disk: elements are serialized on eviction from on-heap cache - off by default */
    // TODO: also allow using for generic elements
    public final boolean ondiskOverflowEnabled;
    protected OndiskOverflow ondiskOverflow;
    protected ReferenceManager referenceManager;

    private TinkerGraph(final Configuration configuration, boolean usesSpecializedElements,
                        Map<String, SpecializedElementFactory.ForVertex> specializedVertexFactoryByLabel,
                        Map<String, SpecializedElementFactory.ForEdge> specializedEdgeFactoryByLabel) {
        this.configuration = configuration;
        this.usesSpecializedElements = usesSpecializedElements;
        this.specializedVertexFactoryByLabel = specializedVertexFactoryByLabel;
        this.specializedEdgeFactoryByLabel = specializedEdgeFactoryByLabel;
        vertexIdManager = new IdManager();

        graphLocation = configuration.getString(GREMLIN_TINKERGRAPH_GRAPH_LOCATION, null);
        ondiskOverflowEnabled = configuration.getBoolean(GREMLIN_TINKERGRAPH_ONDISK_OVERFLOW_ENABLED, true);
        if (ondiskOverflowEnabled) {
            graphFormat = GRAPH_FORMAT_MVSTORE;
            referenceManager = new ReferenceManagerImpl(configuration.getInt(GREMLIN_TINKERGRAPH_OVERFLOW_HEAP_PERCENTAGE_THRESHOLD));
            VertexDeserializer vertexDeserializer = new VertexDeserializer(this, specializedVertexFactoryByLabel);
            EdgeDeserializer edgeDeserializer = new EdgeDeserializer(this, specializedEdgeFactoryByLabel);
            if (graphLocation == null) {
                ondiskOverflow = OndiskOverflow.createWithTempFile(vertexDeserializer, edgeDeserializer);
                initEmptyElementCollections();
            } else {
                ondiskOverflow = OndiskOverflow.createWithSpecificLocation(vertexDeserializer, edgeDeserializer, new File(graphLocation));
                initElementCollections(ondiskOverflow);
            }
        } else {
            graphFormat = configuration.getString(GREMLIN_TINKERGRAPH_GRAPH_FORMAT, null);
            if ((graphLocation != null && null == graphFormat) || (null == graphLocation && graphFormat != null))
                throw new IllegalStateException(String.format("The %s and %s must both be specified if either is present",
                    GREMLIN_TINKERGRAPH_GRAPH_LOCATION, GREMLIN_TINKERGRAPH_GRAPH_FORMAT));
            initEmptyElementCollections();
            referenceManager = new NoOpReferenceManager();
            if (graphLocation != null) loadGraph();
        }
    }

    private void initEmptyElementCollections() {
        vertices = new TLongObjectHashMap<>();
        edges = new TLongObjectHashMap<>();
        verticesByLabel = new THashMap<>(100);
        edgesByLabel = new THashMap<>(100);
    }

    /** implementation note: must start with vertices, because the edges require the vertexRefs to be already present! */
    private void initElementCollections(OndiskOverflow ondiskOverflow) {
        long start = System.currentTimeMillis();
        final Set<Map.Entry<Long, byte[]>> serializedVertices = ondiskOverflow.allVertices();
        final Set<Map.Entry<Long, byte[]>> serializedEdges = ondiskOverflow.allEdges();
        int elementCount = serializedVertices.size() + serializedEdges.size();
        logger.info("initializing " + elementCount + " elements from existing storage - this may take some time");
        int importCount = 0;
        long maxId = currentId.get();

        vertices = new TLongObjectHashMap<>(serializedVertices.size());
        verticesByLabel = new THashMap<>(serializedVertices.size());
        final Iterator<Map.Entry<Long, byte[]>> serializedVertexIter = serializedVertices.iterator();
        while (serializedVertexIter.hasNext()) {
            final Map.Entry<Long, byte[]> entry = serializedVertexIter.next();
            try {
                final VertexRef<TinkerVertex> vertexRef = (VertexRef<TinkerVertex>) ondiskOverflow.getVertexDeserializer().get().deserializeRef(entry.getValue());
                vertices.put(vertexRef.id, vertexRef);
                getElementsByLabel(verticesByLabel, vertexRef.label()).add(vertexRef);
                importCount++;
                if (importCount % 100000 == 0) {
                    logger.debug("imported " + importCount + " elements - still running...");
                }
                if (vertexRef.id > maxId) maxId = vertexRef.id;
            } catch (IOException e) {
                throw new RuntimeException("error while initializing vertex from storage: id=" + entry.getKey(), e);
            }
        }

        edges = new TLongObjectHashMap<>(serializedEdges.size());
        edgesByLabel = new THashMap<>(serializedEdges.size());
        final Iterator<Map.Entry<Long, byte[]>> serializedEdgeIter = serializedEdges.iterator();
        while (serializedEdgeIter.hasNext()) {
            final Map.Entry<Long, byte[]> entry = serializedEdgeIter.next();
            try {
                final EdgeRef<TinkerEdge> edgeRef = (EdgeRef<TinkerEdge>) ondiskOverflow.getEdgeDeserializer().get().deserializeRef(entry.getValue());
                edges.put(edgeRef.id, edgeRef);
                getElementsByLabel(edgesByLabel, edgeRef.label()).add(edgeRef);
                importCount++;
                if (importCount % 100000 == 0) {
                    logger.debug("imported " + importCount + " elements - still running...");
                }
                if (edgeRef.id > maxId) maxId = edgeRef.id;
            } catch (IOException e) {
                throw new RuntimeException("error while initializing edge from storage: id=" + entry.getKey(), e);
            }
        }
        currentId.set(maxId + 1);
        long elapsedMillis = System.currentTimeMillis() - start;
        logger.info("initialized " + this.toString() + " from existing storage in " + elapsedMillis + "ms");
    }

    /**
     * Open a new {@link TinkerGraph} instance.
     * <p/>
     * <b>Reference Implementation Help:</b> If a {@link Graph} implementation does not require a {@code Configuration}
     * (or perhaps has a default configuration) it can choose to implement a zero argument
     * {@code open()} method. This is an optional constructor method for TinkerGraph. It is not enforced by the Gremlin
     * Test Suite.
     */
    public static TinkerGraph open() {
        return open(EMPTY_CONFIGURATION());
    }

    /**
     * Open a new {@code TinkerGraph} instance.
     * <p/>
     * <b>Reference Implementation Help:</b> This method is the one use by the {@link GraphFactory} to instantiate
     * {@link Graph} instances.  This method must be overridden for the Structure Test Suite to pass. Implementers have
     * latitude in terms of how exceptions are handled within this method.  Such exceptions will be considered
     * implementation specific by the test suite as all test generate graph instances by way of
     * {@link GraphFactory}. As such, the exceptions get generalized behind that facade and since
     * {@link GraphFactory} is the preferred method to opening graphs it will be consistent at that level.
     *
     * @param configuration the configuration for the instance
     * @return a newly opened {@link Graph}
     */
    public static TinkerGraph open(final Configuration configuration) {
        return new TinkerGraph(configuration, false, new HashMap<>(), new HashMap<>());
    }


    public static TinkerGraph open(List<SpecializedElementFactory.ForVertex<?>> vertexFactories,
                                   List<SpecializedElementFactory.ForEdge<?>> edgeFactories) {
        return open(EMPTY_CONFIGURATION(), vertexFactories, edgeFactories);
    }

    public static TinkerGraph open(final Configuration configuration,
                                   List<SpecializedElementFactory.ForVertex<?>> vertexFactories,
                                   List<SpecializedElementFactory.ForEdge<?>> edgeFactories) {
        boolean usesSpecializedElements = !vertexFactories.isEmpty() || !edgeFactories.isEmpty();
        Map<String, SpecializedElementFactory.ForVertex> specializedVertexFactoryByLabel = new HashMap<>();
        Map<String, SpecializedElementFactory.ForEdge> specializedEdgeFactoryByLabel = new HashMap<>();
        vertexFactories.forEach(factory -> specializedVertexFactoryByLabel.put(factory.forLabel(), factory));
        edgeFactories.forEach(factory -> specializedEdgeFactoryByLabel.put(factory.forLabel(), factory));
        return new TinkerGraph(configuration, usesSpecializedElements, specializedVertexFactoryByLabel, specializedEdgeFactoryByLabel);
    }

    ////////////// STRUCTURE API METHODS //////////////////

    @Override
    public Vertex addVertex(final Object... keyValues) {
        if (isClosed()) {
            throw new IllegalStateException("cannot add more elements, graph is closed");
        }
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);

        Long idValue = (Long) vertexIdManager.convert(ElementHelper.getIdValue(keyValues).orElse(null));
        if (null != idValue) {
            if (vertices.containsKey(idValue))
                throw Exceptions.vertexWithIdAlreadyExists(idValue);
        } else {
            idValue = (Long) vertexIdManager.getNextId(this);
        }
        currentId.set(Long.max(idValue, currentId.get()));

        final Vertex vertex = createVertex(idValue, label, keyValues);
        vertices.put((long)vertex.id(), vertex);
        getElementsByLabel(verticesByLabel, label).add(vertex);
        return vertex;
    }

    private Vertex createVertex(final long idValue, final String label, final Object... keyValues) {
        final Vertex vertex;
        if (specializedVertexFactoryByLabel.containsKey(label)) {
            final SpecializedElementFactory.ForVertex factory = specializedVertexFactoryByLabel.get(label);
            final SpecializedTinkerVertex underlying = factory.createVertex(idValue, this);
            vertex = factory.createVertexRef(underlying);
        } else { // vertex label not registered for a specialized factory, treating as generic vertex
            if (this.usesSpecializedElements) {
                throw new IllegalArgumentException(
                  "this instance of TinkerGraph uses specialized elements, but doesn't have a factory for label " + label
                    + ". Mixing specialized and generic elements is not (yet) supported");
            }
            vertex = new TinkerVertex(idValue, label, this);
        }
        ElementHelper.attachProperties(vertex, VertexProperty.Cardinality.list, keyValues);
        return vertex;
    }

    @Override
    public <C extends GraphComputer> C compute(final Class<C> graphComputerClass) {
        throw Graph.Exceptions.graphDoesNotSupportProvidedGraphComputer(graphComputerClass);
    }

    @Override
    public GraphComputer compute() {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public Variables variables() {
        if (null == this.variables)
            this.variables = new TinkerGraphVariables();
        return this.variables;
    }

    @Override
    public <I extends Io> I io(final Io.Builder<I> builder) {
        if (builder.requiresVersion(GryoVersion.V1_0) || builder.requiresVersion(GraphSONVersion.V1_0))
            return (I) builder.graph(this).onMapper(mapper -> mapper.addRegistry(TinkerIoRegistryV1d0.instance())).create();
        else if (builder.requiresVersion(GraphSONVersion.V2_0))   // there is no gryo v2
            return (I) builder.graph(this).onMapper(mapper -> mapper.addRegistry(TinkerIoRegistryV2d0.instance())).create();
        else
            return (I) builder.graph(this).onMapper(mapper -> mapper.addRegistry(TinkerIoRegistryV3d0.instance())).create();
    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, "vertices: " + vertices.size() + ", edges: " + edges.size());
    }

    /**
     * if the {@link #GREMLIN_TINKERGRAPH_GRAPH_LOCATION} is set, data in the graph is persisted to that location.
     */
    @Override
    public void close() {
        this.closed = true;
        if (ondiskOverflowEnabled) {
            if (graphLocation != null) referenceManager.clearAllReferences();
            referenceManager.close();
            ondiskOverflow.close();
        } else {
            if (graphLocation != null) saveGraph();
        }
    }

    @Override
    public Transaction tx() {
        throw Exceptions.transactionsNotSupported();
    }

    @Override
    public Configuration configuration() {
        return configuration;
    }

    public Vertex vertex(final Long id) {
        return vertices.get(id);
    }

    @Override
    public Iterator<Vertex> vertices(final Object... ids) {
        return createElementIterator(Vertex.class, vertices, vertexIdManager, ids);
    }

    public Iterator<Vertex> verticesByLabel(final P<String> labelPredicate) {
        return elementsByLabel(verticesByLabel, labelPredicate);
    }

    public Edge edge(final Long id) {
        return edges.get(id);
    }

    @Override
    public Iterator<Edge> edges(final Object... ids) {
        throw new NotImplementedException("");
    }

    /**
     * retrieve the correct by-label map (and create it if it doesn't yet exist)
     */
    protected <E extends Element> Set<E> getElementsByLabel(final THashMap<String, Set<E>> elementsByLabel, final String label) {
        if (!elementsByLabel.containsKey(label))
            elementsByLabel.put(label, new THashSet<>(100000));
        return elementsByLabel.get(label);
    }

    protected <E extends Element> Iterator<E> elementsByLabel(final THashMap<String, Set<E>> elementsByLabel, final P<String> labelPredicate) {
        final MultiIterator<E> multiIterator = new MultiIterator<>();
        for (String label : elementsByLabel.keySet()) {
            if (labelPredicate.test(label)) {
                multiIterator.addIterator(elementsByLabel.get(label).iterator());
            }
        }
        return multiIterator;
    }

    private void loadGraph() {
        final File f = new File(graphLocation);
        if (f.exists() && f.isFile()) {
            try {
                if (graphFormat.equals("graphml")) {
                    io(IoCore.graphml()).readGraph(graphLocation);
                } else if (graphFormat.equals("graphson")) {
                    io(IoCore.graphson()).readGraph(graphLocation);
                } else if (graphFormat.equals("gryo")) {
                    io(IoCore.gryo()).readGraph(graphLocation);
                } else {
                    io(IoCore.createIoBuilder(graphFormat)).readGraph(graphLocation);
                }
            } catch (Exception ex) {
                throw new RuntimeException(String.format("Could not load graph at %s with %s", graphLocation, graphFormat), ex);
            }
        }
    }

    private void saveGraph() {
        final File f = new File(graphLocation);
        if (f.exists()) {
            f.delete();
        } else {
            final File parent = f.getParentFile();

            // the parent would be null in the case of an relative path if the graphLocation was simply: "f.gryo"
            if (parent != null && !parent.exists()) {
                parent.mkdirs();
            }
        }

        try {
            if (graphFormat.equals("graphml")) {
                io(IoCore.graphml()).writeGraph(graphLocation);
            } else if (graphFormat.equals("graphson")) {
                io(IoCore.graphson()).writeGraph(graphLocation);
            } else if (graphFormat.equals("gryo")) {
                io(IoCore.gryo()).writeGraph(graphLocation);
            } else {
                io(IoCore.createIoBuilder(graphFormat)).writeGraph(graphLocation);
            }
        } catch (Exception ex) {
            throw new RuntimeException(String.format("Could not save graph at %s with %s", graphLocation, graphFormat), ex);
        }
    }

    private <T extends Element> Iterator<T> createElementIterator(final Class<T> clazz,
                                                                  final TLongObjectMap<T> elements,
                                                                  final IdManager idManager,
                                                                  final Object... ids) {
        final Iterator<T> iterator;
        if (0 == ids.length) {
            iterator = elements.valueCollection().iterator();
        } else {
            final List<Object> idList = Arrays.asList(ids);
            validateHomogenousIds(idList);

            // if the type is of Element - have to look each up because it might be an Attachable instance or
            // other implementation. the assumption is that id conversion is not required for detached
            // stuff - doesn't seem likely someone would detach a Titan vertex then try to expect that
            // vertex to be findable in OrientDB
            return clazz.isAssignableFrom(ids[0].getClass()) ?
                    IteratorUtils.filter(IteratorUtils.map(idList, id -> elements.get((long)clazz.cast(id).id())).iterator(), Objects::nonNull)
                    : IteratorUtils.filter(IteratorUtils.map(idList, id -> elements.get((long)idManager.convert(id))).iterator(), Objects::nonNull);
        }
        return iterator;
    }

    /**
     * Return TinkerGraph feature set.
     * <p/>
     * <b>Reference Implementation Help:</b> Implementers only need to implement features for which there are
     * negative or instance configured features.  By default, all
     * {@link org.apache.tinkerpop.gremlin.structure.Graph.Features} return true.
     */
    @Override
    public Features features() {
        return features;
    }

    private void validateHomogenousIds(final List<Object> ids) {
        final Iterator<Object> iterator = ids.iterator();
        Object id = iterator.next();
        if (id == null)
            throw Graph.Exceptions.idArgsMustBeEitherIdOrElement();
        final Class firstClass = id.getClass();
        while (iterator.hasNext()) {
            id = iterator.next();
            if (id == null || !id.getClass().equals(firstClass))
                throw Graph.Exceptions.idArgsMustBeEitherIdOrElement();
        }
    }

    public boolean isClosed() {
        return closed;
    }

    public class TinkerGraphFeatures implements Features {

        private final TinkerGraphGraphFeatures graphFeatures = new TinkerGraphGraphFeatures();
        private final TinkerGraphEdgeFeatures edgeFeatures = new TinkerGraphEdgeFeatures();
        private final TinkerGraphVertexFeatures vertexFeatures = new TinkerGraphVertexFeatures();

        private TinkerGraphFeatures() {
        }

        @Override
        public GraphFeatures graph() {
            return graphFeatures;
        }

        @Override
        public EdgeFeatures edge() {
            return edgeFeatures;
        }

        @Override
        public VertexFeatures vertex() {
            return vertexFeatures;
        }

        @Override
        public String toString() {
            return StringFactory.featureString(this);
        }

    }

    public class TinkerGraphVertexFeatures implements Features.VertexFeatures {

        private final TinkerGraphVertexPropertyFeatures vertexPropertyFeatures = new TinkerGraphVertexPropertyFeatures();

        private TinkerGraphVertexFeatures() {
        }

        @Override
        public Features.VertexPropertyFeatures properties() {
            return vertexPropertyFeatures;
        }

        @Override
        public boolean supportsCustomIds() {
            return true;
        }

        @Override
        public boolean willAllowId(final Object id) {
            return vertexIdManager.allow(id);
        }

        @Override
        public VertexProperty.Cardinality getCardinality(final String key) {
            return VertexProperty.Cardinality.single;
        }
    }

    public class TinkerGraphEdgeFeatures implements Features.EdgeFeatures {

        private TinkerGraphEdgeFeatures() {
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }

        @Override
        public boolean willAllowId(final Object id) {
            return false;
        }
    }

    public class TinkerGraphGraphFeatures implements Features.GraphFeatures {

        private TinkerGraphGraphFeatures() {
        }

        @Override
        public boolean supportsConcurrentAccess() {
            return false;
        }

        @Override
        public boolean supportsTransactions() {
            return false;
        }

        @Override
        public boolean supportsThreadedTransactions() {
            return false;
        }

    }

    public class TinkerGraphVertexPropertyFeatures implements Features.VertexPropertyFeatures {

        private TinkerGraphVertexPropertyFeatures() {
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }

        @Override
        public boolean willAllowId(final Object id) {
            return vertexIdManager.allow(id);
        }
    }

    ///////////// GRAPH SPECIFIC INDEXING METHODS ///////////////

    /**
     * Create an index for said element class ({@link Vertex} or {@link Edge}) and said property key.
     * Whenever an element has the specified key mutated, the index is updated.
     * When the index is created, all existing elements are indexed to ensure that they are captured by the index.
     *
     * @param key          the property key to index
     * @param elementClass the element class to index
     * @param <E>          The type of the element class
     */
    public <E extends Element> void createIndex(final String key, final Class<E> elementClass) {
        if (Vertex.class.isAssignableFrom(elementClass)) {
            if (null == this.vertexIndex) this.vertexIndex = new TinkerIndex<>(this, Vertex.class);
            this.vertexIndex.createKeyIndex(key);
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            if (null == this.edgeIndex) this.edgeIndex = new TinkerIndex<>(this, Edge.class);
            this.edgeIndex.createKeyIndex(key);
        } else {
            throw new IllegalArgumentException("Class is not indexable: " + elementClass);
        }
    }

    /**
     * Drop the index for the specified element class ({@link Vertex} or {@link Edge}) and key.
     *
     * @param key          the property key to stop indexing
     * @param elementClass the element class of the index to drop
     * @param <E>          The type of the element class
     */
    public <E extends Element> void dropIndex(final String key, final Class<E> elementClass) {
        if (Vertex.class.isAssignableFrom(elementClass)) {
            if (null != this.vertexIndex) this.vertexIndex.dropKeyIndex(key);
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            if (null != this.edgeIndex) this.edgeIndex.dropKeyIndex(key);
        } else {
            throw new IllegalArgumentException("Class is not indexable: " + elementClass);
        }
    }

    /**
     * Return all the keys currently being index for said element class  ({@link Vertex} or {@link Edge}).
     *
     * @param elementClass the element class to get the indexed keys for
     * @param <E>          The type of the element class
     * @return the set of keys currently being indexed
     */
    public <E extends Element> Set<String> getIndexedKeys(final Class<E> elementClass) {
        if (Vertex.class.isAssignableFrom(elementClass)) {
            return null == this.vertexIndex ? Collections.emptySet() : this.vertexIndex.getIndexedKeys();
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            return null == this.edgeIndex ? Collections.emptySet() : this.edgeIndex.getIndexedKeys();
        } else {
            throw new IllegalArgumentException("Class is not indexable: " + elementClass);
        }
    }

    /**
     * TinkerGraph will use an implementation of this interface to generate identifiers when a user does not supply
     * them and to handle identifier conversions when querying to provide better flexibility with respect to
     * handling different data types that mean the same thing.
     */
    class IdManager {
        /**
         * Generate an identifier which should be unique to the {@link TinkerGraph} instance.
         */
        public Long getNextId(final TinkerGraph graph) {
            return Stream.generate(() -> (graph.currentId.incrementAndGet())).findAny().get();
        }

        /**
         * Convert an identifier to the type required by the manager.
         */
        public Object convert(final Object id) {
            if (null == id)
                return null;
            else if (id instanceof Long)
                return id;
            else if (id instanceof Number)
                return ((Number) id).longValue();
            else if (id instanceof String)
                return Long.parseLong((String) id);
            else
                throw new IllegalArgumentException(String.format("Expected an id that is convertible to Long but received %s", id.getClass()));
        }

        /**
         * Determine if an identifier is allowed by this manager given its type.
         */
        public boolean allow(final Object id) {
            return id instanceof Number || id instanceof String;
        }
    }
}
