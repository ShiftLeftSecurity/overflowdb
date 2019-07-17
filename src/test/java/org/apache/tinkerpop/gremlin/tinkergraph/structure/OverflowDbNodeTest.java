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

import org.apache.commons.lang3.NotImplementedException;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.__;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class OverflowDbNodeTest {

  @Test
  public void simpleTest() {
    TinkerGraph graph = newGraph();

    Vertex v0 = graph.addVertex(
        T.label, OverflowDbTestNode.label,
        OverflowDbTestNode.STRING_PROPERTY, "node 1",
        OverflowDbTestNode.INT_PROPERTY, 42,
        OverflowDbTestNode.STRING_LIST_PROPERTY, Arrays.asList("stringOne", "stringTwo"),
        OverflowDbTestNode.INT_LIST_PROPERTY, Arrays.asList(42, 43));
    Vertex v1 = graph.addVertex(
        T.label, OverflowDbTestNode.label,
        OverflowDbTestNode.STRING_PROPERTY, "node 2",
        OverflowDbTestNode.INT_PROPERTY, 52,
        OverflowDbTestNode.STRING_LIST_PROPERTY, Arrays.asList("stringThree", "stringFour"),
        OverflowDbTestNode.INT_LIST_PROPERTY, Arrays.asList(52, 53));
    Edge e = v0.addEdge(OverflowDbTestEdge.label, v1, OverflowDbTestEdge.LONG_PROPERTY, 99l);

    // vertex traversals
    assertEquals(1, __(v0).out().toList().size());
    assertEquals(0, __(v0).out("otherLabel").toList().size());
    assertEquals(0, __(v1).out().toList().size());
    assertEquals(0, __(v0).in().toList().size());
    assertEquals(1, __(v1).in().toList().size());
    assertEquals(1, __(v0).both().toList().size());
    assertEquals(1, __(v1).both().toList().size());

    // edge traversals
    assertEquals(1, __(v0).outE().toList().size());
    assertEquals(OverflowDbTestEdge.label, __(v0).outE().label().next());
    assertEquals(0, __(v0).outE("otherLabel").toList().size());
    assertEquals(0, __(v1).outE().toList().size());
    assertEquals(1, __(v1).inE().toList().size());
    assertEquals(1, __(v0).bothE().toList().size());
    assertEquals(1, __(v0).bothE(OverflowDbTestEdge.label).toList().size());
    assertEquals(0, __(v0).bothE("otherLabel").toList().size());

    // vertex properties
    Set stringProperties = graph.traversal().V().values(OverflowDbTestNode.STRING_PROPERTY).toSet();
    assertTrue(stringProperties.contains("node 1"));
    assertTrue(stringProperties.contains("node 2"));
    assertEquals(Integer.valueOf(42), __(e).outV().values(OverflowDbTestNode.INT_PROPERTY).next());
    assertEquals(Integer.valueOf(52), __(e).inV().values(OverflowDbTestNode.INT_PROPERTY).next());

    // edge properties
    assertTrue(e instanceof OverflowDbTestEdge);
    assertEquals(Long.valueOf(99l), ((OverflowDbTestEdge) e).longProperty());
    assertEquals(Long.valueOf(99l), e.value(OverflowDbTestEdge.LONG_PROPERTY));
    assertEquals(Long.valueOf(99l), __(v0).outE().values(OverflowDbTestEdge.LONG_PROPERTY).next());
    assertEquals(Long.valueOf(99l), __(v1).inE().values(OverflowDbTestEdge.LONG_PROPERTY).next());
    assertEquals(Long.valueOf(99l), __(v1).inE().values().next());
  }

  @Test
  public void testEdgeEquality() {
    TinkerGraph graph = newGraph();

    Vertex v0 = graph.addVertex(T.label, OverflowDbTestNode.label);
    Vertex v1 = graph.addVertex(T.label, OverflowDbTestNode.label);

    Edge e0 = v0.addEdge(OverflowDbTestEdge.label, v1, OverflowDbTestEdge.LONG_PROPERTY, 99l);


    Edge e0FromOut = v0.edges(Direction.OUT).next();
    Edge e0FromIn = v1.edges(Direction.IN).next();

    assertEquals(e0, e0FromOut);
    assertEquals(e0, e0FromIn);
    assertEquals(e0FromOut, e0FromIn);
  }

  @Test
  public void setAndGetEdgePropertyViaNewEdge() {
    TinkerGraph graph = newGraph();

    Vertex v0 = graph.addVertex(T.label, OverflowDbTestNode.label);
    Vertex v1 = graph.addVertex(T.label, OverflowDbTestNode.label);

    Edge e0 = v0.addEdge(OverflowDbTestEdge.label, v1);
    e0.property(OverflowDbTestEdge.LONG_PROPERTY, 1L);
    assertEquals(Long.valueOf(1L), e0.property(OverflowDbTestEdge.LONG_PROPERTY).value());
  }

  @Test
  public void setAndGetEdgePropertyViaQueriedEdge() {
    TinkerGraph graph = newGraph();

    Vertex v0 = graph.addVertex(T.label, OverflowDbTestNode.label);
    Vertex v1 = graph.addVertex(T.label, OverflowDbTestNode.label);

    v0.addEdge(OverflowDbTestEdge.label, v1);

    Edge e0 = v0.edges(Direction.OUT, OverflowDbTestEdge.label).next();
    e0.property(OverflowDbTestEdge.LONG_PROPERTY, 1L);
    assertEquals(Long.valueOf(1L), e0.property(OverflowDbTestEdge.LONG_PROPERTY).value());
  }

  @Test
  public void setAndGetEdgePropertyViaDifferenceQueriedEdges() {
    TinkerGraph graph = newGraph();

    Vertex v0 = graph.addVertex(T.label, OverflowDbTestNode.label);
    Vertex v1 = graph.addVertex(T.label, OverflowDbTestNode.label);

    v0.addEdge(OverflowDbTestEdge.label, v1);

    Edge e0ViaOut = v0.edges(Direction.OUT, OverflowDbTestEdge.label).next();
    e0ViaOut.property(OverflowDbTestEdge.LONG_PROPERTY, 1L);

    Edge e0ViaIn = v1.edges(Direction.IN, OverflowDbTestEdge.label).next();
    assertEquals(Long.valueOf(1L), e0ViaIn.property(OverflowDbTestEdge.LONG_PROPERTY).value());
  }

  @Test
  public void setAndGetEdgePropertyViaNewEdgeMultiple() {
    TinkerGraph graph = newGraph();

    Vertex v0 = graph.addVertex(T.label, OverflowDbTestNode.label);
    Vertex v1 = graph.addVertex(T.label, OverflowDbTestNode.label);

    Edge e0 = v0.addEdge(OverflowDbTestEdge.label, v1);
    Edge e1 = v0.addEdge(OverflowDbTestEdge.label, v1);

    e0.property(OverflowDbTestEdge.LONG_PROPERTY, 1L);
    e1.property(OverflowDbTestEdge.LONG_PROPERTY, 2L);

    assertEquals(Long.valueOf(1L), e0.property(OverflowDbTestEdge.LONG_PROPERTY).value());
    assertEquals(Long.valueOf(2L), e1.property(OverflowDbTestEdge.LONG_PROPERTY).value());
  }

  @Test
  public void setAndGetEdgePropertyViaQueriedEdgeMultiple() {
    TinkerGraph graph = newGraph();

    Vertex v0 = graph.addVertex(T.label, OverflowDbTestNode.label);
    Vertex v1 = graph.addVertex(T.label, OverflowDbTestNode.label);

    v0.addEdge(OverflowDbTestEdge.label, v1);
    v0.addEdge(OverflowDbTestEdge.label, v1);

    Iterator<Edge> edgeIt = v0.edges(Direction.OUT, OverflowDbTestEdge.label);

    Edge e0 = edgeIt.next();
    Edge e1 = edgeIt.next();

    e0.property(OverflowDbTestEdge.LONG_PROPERTY, 1L);
    e1.property(OverflowDbTestEdge.LONG_PROPERTY, 2L);

    assertEquals(Long.valueOf(1L), e0.property(OverflowDbTestEdge.LONG_PROPERTY).value());
    assertEquals(Long.valueOf(2L), e1.property(OverflowDbTestEdge.LONG_PROPERTY).value());
  }

  @Test
  public void setAndGetEdgePropertyViaDifferenceQueriedEdgesMultiple() {
    TinkerGraph graph = newGraph();

    Vertex v0 = graph.addVertex(T.label, OverflowDbTestNode.label);
    Vertex v1 = graph.addVertex(T.label, OverflowDbTestNode.label);

    v0.addEdge(OverflowDbTestEdge.label, v1);
    v0.addEdge(OverflowDbTestEdge.label, v1);

    Iterator<Edge> outEdgeIt = v0.edges(Direction.OUT, OverflowDbTestEdge.label);
    Iterator<Edge> inEdgeIt = v1.edges(Direction.IN, OverflowDbTestEdge.label);

    Edge e0ViaOut = outEdgeIt.next();
    Edge e1ViaOut = outEdgeIt.next();
    Edge e0ViaIn = inEdgeIt.next();
    Edge e1ViaIn = inEdgeIt.next();

    e0ViaOut.property(OverflowDbTestEdge.LONG_PROPERTY, 1L);
    e1ViaOut.property(OverflowDbTestEdge.LONG_PROPERTY, 2L);

    assertEquals(Long.valueOf(1L), e0ViaIn.property(OverflowDbTestEdge.LONG_PROPERTY).value());
    assertEquals(Long.valueOf(2L), e1ViaIn.property(OverflowDbTestEdge.LONG_PROPERTY).value());
  }

  private TinkerGraph newGraph() {
    return TinkerGraph.open(
        Arrays.asList(OverflowDbTestNode.factory),
        Arrays.asList(OverflowDbTestEdge.factory)
    );
  }

//    @Test
//    public void simpleTest() {
//        TinkerGraph graph = newGratefulDeadGraphWithSpecializedElements();
//
//        Vertex v0 = graph.addVertex(T.label, Song.label, Song.NAME, "Song 1");
//        Vertex v2 = graph.addVertex(T.label, Song.label, Song.NAME, "Song 2");
//        v0.addEdge(FollowedBy.label, v2);
//
//        Set<Object> songNames = graph.traversal().V().values(Song.NAME).toSet();
//        assertTrue(songNames.contains("Song 1"));
//        assertTrue(songNames.contains("Song 2"));
//
//        assertEquals(1, __(v0).bothE().toList().size());
//        assertEquals(1, __(v0).bothE(FollowedBy.label).toList().size());
//        assertEquals(0, __(v0).bothE("otherLabel").toList().size());
//        assertEquals(1, __(v0).out().toList().size());
//        assertEquals(0, __(v2).out().toList().size());
//        assertEquals(0, __(v0).in().toList().size());
//        assertEquals(1, __(v2).in().toList().size());
//        assertEquals(1, __(v0).both().toList().size());
//        assertEquals(1, __(v2).both().toList().size());
//    }

//    @Test
//    public void shouldAllowToSpecifyIds() {
//        TinkerGraph graph = newGratefulDeadGraphWithSpecializedElements();
//
//        Vertex v10 = graph.addVertex(T.id, 10l, T.label, Song.label, Song.NAME, "Song 10");
//        Vertex v20 = graph.addVertex(T.id, 20l, T.label, Song.label, Song.NAME, "Song 20");
//        v10.addEdge(FollowedBy.label, v20, T.id, 30l, FollowedBy.WEIGHT, 5);
//
//        assertEquals(30l, graph.traversal().V(10l).outE(FollowedBy.label).id().next());
//        assertEquals(5, graph.traversal().V(10l).outE(FollowedBy.label).values(FollowedBy.WEIGHT).next());
//        assertEquals(5, graph.traversal().V(20l).inE(FollowedBy.label).values(FollowedBy.WEIGHT).next());
//    }
//
//    @Test
//    public void shouldReturnElementRefs() {
//        TinkerGraph graph = newGratefulDeadGraphWithSpecializedElements();
//
//        Vertex v0 = graph.addVertex(T.label, Song.label, Song.NAME, "Song 1");
//        Vertex v2 = graph.addVertex(T.label, Song.label, Song.NAME, "Song 2");
//        Edge e4 = v0.addEdge(FollowedBy.label, v2);
//        assertTrue(v0 instanceof VertexRef);
////        assertTrue(e4 instanceof EdgeRef); TODO
////        assertTrue(v0.edges(Direction.OUT).next() instanceof EdgeRef);
//        assertTrue(v0.vertices(Direction.OUT).next() instanceof VertexRef);
//    }
//
//    @Test
//    /* ensure these are identical for both ondisk overflow enabled/disabled */
//    public void optimizationStrategyAffectedSteps() throws IOException {
//        TinkerGraph graph = newGratefulDeadGraphWithSpecializedElementsWithData();
//
//        // using `g.V().hasLabel(lbl)` optimization
//        assertEquals(584, graph.traversal().V().hasLabel(Song.label).toList().size());
//        assertEquals(142, graph.traversal().V().has(Song.PERFORMANCES, 1).toList().size());
//        assertEquals(142, graph.traversal().V().has(Song.PERFORMANCES, 1).hasLabel(Song.label).toList().size());
//        assertEquals(142, graph.traversal().V().hasLabel(Song.label).has(Song.PERFORMANCES, 1).toList().size());
//        assertEquals(7047, graph.traversal().V().out().hasLabel(Song.label).toList().size());
//        assertEquals(1, graph.traversal().V(800l).hasLabel(Song.label).toList().size());
//        assertEquals(5, graph.traversal().V(1l).out().hasLabel(Song.label).toList().size());
//        assertEquals(0, graph.traversal().V().hasLabel(Song.label).hasLabel(Artist.label).toList().size());
//        assertEquals(808, graph.traversal().V().hasLabel(Song.label, Artist.label).toList().size());
//        assertEquals(501, graph.traversal().V().outE().hasLabel(WrittenBy.label).toList().size());
//        assertEquals(501, graph.traversal().V().hasLabel(Song.label).outE().hasLabel(WrittenBy.label).toList().size());
//
//        // using `g.E().hasLabel(lbl)` optimization
//        assertEquals(8049, graph.traversal().E().toList().size());
//        assertEquals(7047, graph.traversal().E().hasLabel(FollowedBy.label).toList().size());
//        assertEquals(3564, graph.traversal().E().has(FollowedBy.WEIGHT, 1).toList().size());
//        assertEquals(3564, graph.traversal().E().hasLabel(FollowedBy.label).has(FollowedBy.WEIGHT, 1).toList().size());
//        assertEquals(3564, graph.traversal().E().has(FollowedBy.WEIGHT, 1).hasLabel(FollowedBy.label).toList().size());
//        assertEquals(7047, graph.traversal().E().hasLabel(FollowedBy.label).outV().hasLabel(Song.label).toList().size());
//        assertEquals(1, graph.traversal().E(0l).hasLabel(FollowedBy.label).toList().size());
//        assertEquals(7548, graph.traversal().E().hasLabel(FollowedBy.label, SungBy.label).toList().size());
//
//        graph.close();
//    }
//
//    @Test
//    public void gratefulDeadGraph() throws IOException {
//        TinkerGraph graph = newGratefulDeadGraphWithSpecializedElementsWithData();
//
//        List<Vertex> garcias = graph.traversal().V().has("name", "Garcia").toList();
//        assertEquals(garcias.size(), 1);
//        VertexRef vertexRef = (VertexRef) garcias.get(0); // Tinkergraph returns VertexRefs for overflow
//        Artist garcia = (Artist) vertexRef.get(); //it's actually of type `Artist`, not (only) `Vertex`
//        assertEquals("Garcia", garcia.getName());
//        graph.close();
//    }
//
//    @Test
//    public void testBasicSteps() throws IOException {
//        TinkerGraph graph = newGratefulDeadGraphWithSpecializedElementsWithData();
//        Vertex garcia = graph.traversal().V().has("name", "Garcia").next();
//
//        // inE
//        assertEquals(4, __(garcia).inE(WrittenBy.label).toList().size());
//        assertEquals(4, __(garcia).inE(WrittenBy.label).outV().toList().size());
//
//        // in
//        assertEquals(4, __(garcia).in(WrittenBy.label).toList().size());
//        List<Vertex> songsWritten = __(garcia).in(WrittenBy.label).has("name", "CREAM PUFF WAR").toList();
//        assertEquals(songsWritten.size(), 1);
//        VertexRef<Song> songRef = (VertexRef) songsWritten.get(0); //it's actually of type `VertexRef<Song>`, but we can't infer that since it's behind the tinkerpop api
//        Song song = songRef.get();
//        assertEquals("CREAM PUFF WAR", song.getName());
//
//        // outE
//        assertEquals(1, __(song).outE(WrittenBy.label).toList().size());
//
//        // out
//        List<Vertex> songOut = __(song).out(WrittenBy.label).toList();
//        assertEquals(1, songOut.size());
//        assertEquals(garcia, songOut.get(0));
//
//        // bothE
//        List<Edge> songBothE = __(song).bothE(WrittenBy.label).toList();
//        assertEquals(1, songBothE.size());
//
//        // both
//        List<Vertex> songBoth = __(song).both(WrittenBy.label).toList();
//        assertEquals(1, songBoth.size());
//        assertEquals(garcia, songBoth.get(0));
//        graph.close();
//    }
//
//    @Test
//    public void shouldAllowAddingElementsAndSettingProperties() throws IOException {
//        TinkerGraph graph = newGratefulDeadGraphWithSpecializedElements();
//
//        Vertex song1 = graph.addVertex(Song.label);
//        Vertex song2 = graph.addVertex(Song.label);
//        song1.property(Song.NAME, "song 1");
//        song2.property(Song.NAME, "song 2");
//
//        List<Vertex> vertices = graph.traversal().V().toList();
//        assertEquals(2, vertices.size());
//        Set<Object> names = graph.traversal().V().values("name").toSet();
//        assertTrue(names.contains("song 1"));
//        assertTrue(names.contains("song 2"));
//
//        song1.addEdge(FollowedBy.label, song2, FollowedBy.WEIGHT, new Integer(42));
//        assertEquals(42, graph.traversal().E().values(FollowedBy.WEIGHT).next());
//        assertEquals(42, __(song1).outE().values(FollowedBy.WEIGHT).next());
//
//        graph.close();
//    }
//
//    @Test
//    public void shouldSupportEdgeRemoval() {
//        TinkerGraph graph = newGratefulDeadGraphWithSpecializedElements();
//        Vertex song1 = graph.addVertex(Song.label);
//        Vertex song2 = graph.addVertex(Song.label);
//        Edge followedBy = song1.addEdge(FollowedBy.label, song2);
//        assertEquals(2, graph.traversal().V().toList().size());
//        assertEquals(1, graph.traversal().E().toList().size());
//
//        followedBy.remove();
//        assertEquals(2, graph.traversal().V().toList().size());
//        assertEquals(0, graph.traversal().E().toList().size());
//
//        graph.close();
//    }
//
//    @Test
//    public void shouldSupportVertexRemoval1() {
//        TinkerGraph graph = newGratefulDeadGraphWithSpecializedElements();
//        Vertex song1 = graph.addVertex(Song.label);
//        Vertex song2 = graph.addVertex(Song.label);
//        song1.addEdge(FollowedBy.label, song2);
//        assertEquals(2, graph.traversal().V().toList().size());
//        assertEquals(1, graph.traversal().E().toList().size());
//
//        song1.remove();
//        assertEquals(1, graph.traversal().V().toList().size());
//        assertEquals(0, graph.traversal().E().toList().size());
//
//        song2.remove();
//        assertEquals(0, graph.traversal().V().toList().size());
//
//        graph.close();
//    }
//
//    @Test
//    public void shouldSupportVertexRemoval2() {
//        TinkerGraph graph = newGratefulDeadGraphWithSpecializedElements();
//        Vertex song1 = graph.addVertex(Song.label);
//        Vertex song2 = graph.addVertex(Song.label);
//        song1.addEdge(FollowedBy.label, song2);
//        assertEquals(2, graph.traversal().V().toList().size());
//        assertEquals(1, graph.traversal().E().toList().size());
//
//        song2.remove();
//        assertEquals(1, graph.traversal().V().toList().size());
//        assertEquals(0, graph.traversal().E().toList().size());
//
//        song1.remove();
//        assertEquals(0, graph.traversal().V().toList().size());
//
//        graph.close();
//    }
//
//    @Test
//    public void shouldSupportRemovalOfSpecializedElementsInBigDataset() throws IOException {
//        TinkerGraph graph = newGratefulDeadGraphWithSpecializedElementsWithData();
//        Set<Vertex> garcias = graph.traversal().V().has("name", "Garcia").toSet();
//        assertNotEquals(0, garcias.size());
//        garcias.forEach(Element::remove);
//        Long garciaCount = graph.traversal().V().has("name", "Garcia").count().next();
//        assertEquals(Long.valueOf(0), garciaCount);
//
//        List<Vertex> outVertices = graph.traversal().E().outV().toList();
//        outVertices.forEach(outVertex -> assertFalse(garcias.contains(outVertex)));
//
//        List<Vertex> inVertices = graph.traversal().E().inV().toList();
//        inVertices.forEach(inVertex -> assertFalse(garcias.contains(inVertex)));
//        graph.close();
//    }
//
//    @Test
//    public void shouldNotAllowMixingWithGenericVertex() throws IOException {
//        boolean caughtException = false;
//        TinkerGraph graph = newGratefulDeadGraphWithSpecializedElementsWithData();
//        try {
//            graph.addVertex("something_else");
//        } catch (IllegalArgumentException e) {
//            caughtException = true;
//        }
//        graph.close();
//        assertTrue(caughtException);
//    }
//
//    @Test
//    public void shouldNotAllowMixingWithGenericEdge() throws IOException {
//        TinkerGraph graph = newGratefulDeadGraphWithSpecializedElementsWithData();
//        boolean caughtException = false;
//        try {
//            GraphTraversalSource g = graph.traversal();
//            List<Vertex> vertices = g.V().limit(2).toList();
//            Vertex v1 = vertices.get(0);
//            Vertex v2 = vertices.get(1);
//            v1.addEdge("something_else", v2);
//        } catch (IllegalArgumentException e) {
//            caughtException = true;
//        }
//        graph.close();
//        assertTrue(caughtException);
//    }
//
//    @Test
//    @Ignore // only run manually since the timings vary depending on the environment
//    public void shouldUseIndices() throws IOException {
//        int loops = 100;
//        Double avgTimeWithIndex = null;
//        Double avgTimeWithoutIndex = null;
//
//        { // tests with index
//            TinkerGraph graph = newGratefulDeadGraphWithSpecializedElementsWithData();
//            graph.createIndex("weight", Edge.class);
//            GraphTraversalSource g = graph.traversal();
//            assertEquals(3564, (long) g.E().has("weight", P.eq(1)).count().next());
//            avgTimeWithIndex = TimeUtil.clock(loops, () -> g.E().has("weight", P.eq(1)).count().next());
//            graph.close();
//        }
//
//        { // tests without index
//            TinkerGraph graph = newGratefulDeadGraphWithSpecializedElementsWithData();
//            GraphTraversalSource g = graph.traversal();
//            assertEquals(3564, (long) g.E().has("weight", P.eq(1)).count().next());
//            avgTimeWithoutIndex = TimeUtil.clock(loops, () -> g.E().has("weight", P.eq(1)).count().next());
//            graph.close();
//        }
//
//        System.out.println("avgTimeWithIndex = " + avgTimeWithIndex);
//        System.out.println("avgTimeWithoutIndex = " + avgTimeWithoutIndex);
//        assertTrue("avg time with index should be (significantly) less than without index",
//            avgTimeWithIndex < avgTimeWithoutIndex);
//    }
//
//    @Test
//    @Ignore // only run manually since the timings vary depending on the environment
//    public void shouldUseIndicesCreatedBeforeLoadingData() throws IOException {
//        int loops = 100;
//        Double avgTimeWithIndex = null;
//        Double avgTimeWithoutIndex = null;
//
//        { // tests with index
//            TinkerGraph graph = newGratefulDeadGraphWithSpecializedElements();
//            graph.createIndex("weight", Edge.class);
//            loadGraphMl(graph);
//            GraphTraversalSource g = graph.traversal();
//            assertEquals(3564, (long) g.E().has("weight", P.eq(1)).count().next());
//            avgTimeWithIndex = TimeUtil.clock(loops, () -> g.E().has("weight", P.eq(1)).count().next());
//            graph.close();
//        }
//
//        { // tests without index
//            TinkerGraph graph = newGratefulDeadGraphWithSpecializedElementsWithData();
//            GraphTraversalSource g = graph.traversal();
//            assertEquals(3564, (long) g.E().has("weight", P.eq(1)).count().next());
//            avgTimeWithoutIndex = TimeUtil.clock(loops, () -> g.E().has("weight", P.eq(1)).count().next());
//            graph.close();
//        }
//
//        System.out.println("avgTimeWithIndex = " + avgTimeWithIndex);
//        System.out.println("avgTimeWithoutIndex = " + avgTimeWithoutIndex);
//        assertTrue("avg time with index should be (significantly) less than without index",
//            avgTimeWithIndex < avgTimeWithoutIndex);
//    }
//
//    @Test
//    public void handleEmptyProperties() throws IOException {
//        TinkerGraph graph = newGratefulDeadGraphWithSpecializedElementsWithData();
//
//        List<Object> props1 = graph.traversal().V().values("foo").toList();
//        List<Object> props2 = graph.traversal().E().values("foo").toList();
//        // results will be empty, but it should't crash. see https://github.com/ShiftLeftSecurity/tinkergraph-gremlin/issues/12
//        assertEquals(props1.size(), 0);
//        assertEquals(props2.size(), 0);
//        graph.close();
//    }
//
//    //     @Test
//    // only run manually since the timings vary depending on the environment
//    public void propertyLookupPerformanceComparison() throws IOException {
//        int loops = 1000;
//        Double avgTimeWithSpecializedElements = null;
//        Double avgTimeWithGenericElements = null;
//
//        { // using specialized elements
//            TinkerGraph graph = newGratefulDeadGraphWithSpecializedElementsWithData();
//            GraphTraversalSource g = graph.traversal();
//            assertEquals(3564, (long) g.E().has("weight", P.eq(1)).count().next());
//            avgTimeWithSpecializedElements = TimeUtil.clock(loops, () -> g.E().has("weight", P.eq(1)).count().next());
//            graph.close();
//        }
//
//        { // using generic elements
//            TinkerGraph graph = newGratefulDeadGraphWithGenericElementsWithData();
//            GraphTraversalSource g = graph.traversal();
//            assertEquals(3564, (long) g.E().has("weight", P.eq(1)).count().next());
//            avgTimeWithGenericElements = TimeUtil.clock(loops, () -> g.E().has("weight", P.eq(1)).count().next());
//            graph.close();
//        }
//
//        System.out.println("avgTimeWithSpecializedElements = " + avgTimeWithSpecializedElements);
//        System.out.println("avgTimeWithGenericElements = " + avgTimeWithGenericElements);
//
//        double diffPercent = (avgTimeWithGenericElements - avgTimeWithSpecializedElements) / avgTimeWithGenericElements;
//        System.out.println("performance enhancement of specialized elements = " + diffPercent);
//
//        assertTrue("avg time with specialized elements should be less than with generic elements",
//            avgTimeWithSpecializedElements < avgTimeWithGenericElements);
//    }
//
//    //    @Test
//    // only run manually since the timings vary depending on the environment
//    public void traversalPerformanceComparison() throws IOException {
//        int loops = 1000;
//        Double avgTimeWithSpecializedElements = null;
//        Double avgTimeWithGenericElements = null;
//
//        { // using specialized elements
//            TinkerGraph graph = newGratefulDeadGraphWithSpecializedElementsWithData();
//            GraphTraversalSource g = graph.traversal();
//            avgTimeWithSpecializedElements = TimeUtil.clock(loops, () -> g.V().out().out().out().toStream().count());
//            graph.close();
//        }
//
//        { // using generic elements
//            TinkerGraph graph = newGratefulDeadGraphWithGenericElementsWithData();
//            GraphTraversalSource g = graph.traversal();
//            avgTimeWithGenericElements = TimeUtil.clock(loops, () -> g.V().out().out().out().toStream().count());
//            graph.close();
//        }
//
//        System.out.println("avgTimeWithSpecializedElements = " + avgTimeWithSpecializedElements);
//        System.out.println("avgTimeWithGenericElements = " + avgTimeWithGenericElements);
//
//        double diffPercent = (avgTimeWithGenericElements - avgTimeWithSpecializedElements) / avgTimeWithGenericElements;
//        System.out.println("performance enhancement of specialized elements = " + diffPercent);
//    }
//
//    private TinkerGraph newGratefulDeadGraphWithSpecializedElements() {
//        Configuration configuration = TinkerGraph.EMPTY_CONFIGURATION();
//        configuration.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_ONDISK_OVERFLOW_ENABLED, false);
//        return TinkerGraph.open(
//            configuration,
//            Arrays.asList(Song.factory, Artist.factory),
//            Arrays.asList(FollowedBy.factory, SungBy.factory, WrittenBy.factory)
//        );
//    }
//
//    private TinkerGraph newGratefulDeadGraphWithSpecializedElementsWithData() throws IOException {
//        TinkerGraph graph = newGratefulDeadGraphWithSpecializedElements();
//        loadGraphMl(graph);
//        return graph;
//    }

}
