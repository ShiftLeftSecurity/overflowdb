package io.shiftleft.overflowdb;

import io.shiftleft.overflowdb.testdomains.gratefuldead.Artist;
import io.shiftleft.overflowdb.testdomains.gratefuldead.FollowedBy;
import io.shiftleft.overflowdb.testdomains.gratefuldead.GratefulDead;
import io.shiftleft.overflowdb.testdomains.gratefuldead.Song;
import io.shiftleft.overflowdb.testdomains.simple.TestEdge;
import io.shiftleft.overflowdb.testdomains.simple.TestNode;
import io.shiftleft.overflowdb.testdomains.simple.SimpleDomain;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.__;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OdbNodeTest {

  @Test
  public void simpleTest() {
    try (OdbGraph graph = SimpleDomain.newGraph()) {
      Vertex v0 = graph.addVertex(
          T.label, TestNode.LABEL,
          TestNode.STRING_PROPERTY, "node 1",
          TestNode.INT_PROPERTY, 42,
          TestNode.STRING_LIST_PROPERTY, Arrays.asList("stringOne", "stringTwo"),
          TestNode.INT_LIST_PROPERTY, Arrays.asList(42, 43));
      Vertex v1 = graph.addVertex(
          T.label, TestNode.LABEL,
          TestNode.STRING_PROPERTY, "node 2",
          TestNode.INT_PROPERTY, 52,
          TestNode.STRING_LIST_PROPERTY, Arrays.asList("stringThree", "stringFour"),
          TestNode.INT_LIST_PROPERTY, Arrays.asList(52, 53));
      Edge e = v0.addEdge(TestEdge.LABEL, v1, TestEdge.LONG_PROPERTY, 99l);

      //  tinkerpop api returns generic `vertex|edge`, but we can safely cast
      TestNode node1 = (TestNode) v0;
      assertEquals("node 1", node1.stringProperty());
      assertEquals(Integer.valueOf(42), node1.intProperty());
      TestEdge testEdge = (TestEdge) e;
      assertEquals(Long.valueOf(99), testEdge.longProperty());

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
      assertEquals(TestEdge.LABEL, __(v0).outE().label().next());
      assertEquals(0, __(v0).outE("otherLabel").toList().size());
      assertEquals(0, __(v1).outE().toList().size());
      assertEquals(1, __(v1).inE().toList().size());
      assertEquals(1, __(v0).bothE().toList().size());
      assertEquals(1, __(v0).bothE(TestEdge.LABEL).toList().size());
      assertEquals(0, __(v0).bothE("otherLabel").toList().size());

      // vertex properties
      Set stringProperties = graph.traversal().V().values(TestNode.STRING_PROPERTY).toSet();
      assertTrue(stringProperties.contains("node 1"));
      assertTrue(stringProperties.contains("node 2"));
      assertEquals(42, __(e).outV().values(TestNode.INT_PROPERTY).next());
      assertEquals(52, __(e).inV().values(TestNode.INT_PROPERTY).next());

      // edge properties
      assertTrue(e instanceof TestEdge);
      assertEquals(Long.valueOf(99l), ((TestEdge) e).longProperty());
      assertEquals(Long.valueOf(99l), e.value(TestEdge.LONG_PROPERTY));
      assertEquals(99l, __(v0).outE().values(TestEdge.LONG_PROPERTY).next());
      assertEquals(99l, __(v1).inE().values(TestEdge.LONG_PROPERTY).next());
      assertEquals(99l, __(v1).inE().values().next());
    }
  }

  @Test
  public void loadGratefulDeadGraph() throws IOException {
    try(OdbGraph graph = GratefulDead.newGraphWithData()) {
      Vertex v1 = graph.traversal().V(1l).next();
      assertEquals("HEY BO DIDDLEY", v1.value("name"));

      List<Vertex> garcias = graph.traversal().V().has("name", "Garcia").toList();
      assertEquals(garcias.size(), 1);
      NodeRef nodeRef = (NodeRef) garcias.get(0);
      Artist garcia = (Artist) nodeRef.get(); //it's actually of type `Artist`, not (only) `Vertex`
      assertEquals("Garcia", garcia.getName());
    }
  }

  @Test
  public void testEdgeEquality() {
    OdbGraph graph = SimpleDomain.newGraph();

    Vertex v0 = graph.addVertex(T.label, TestNode.LABEL);
    Vertex v1 = graph.addVertex(T.label, TestNode.LABEL);

    Edge e0 = v0.addEdge(TestEdge.LABEL, v1, TestEdge.LONG_PROPERTY, 99l);

    Edge e0FromOut = v0.edges(Direction.OUT).next();
    Edge e0FromIn = v1.edges(Direction.IN).next();

    assertEquals(e0, e0FromOut);
    assertEquals(e0, e0FromIn);
    assertEquals(e0FromOut, e0FromIn);
  }

  @Test
  public void setAndGetEdgePropertyViaNewEdge() {
    OdbGraph graph = SimpleDomain.newGraph();

    Vertex v0 = graph.addVertex(T.label, TestNode.LABEL);
    Vertex v1 = graph.addVertex(T.label, TestNode.LABEL);

    Edge e0 = v0.addEdge(TestEdge.LABEL, v1);
    e0.property(TestEdge.LONG_PROPERTY, 1L);
    assertEquals(1L, e0.property(TestEdge.LONG_PROPERTY).value());
  }

  @Test
  public void setAndGetEdgePropertyViaQueriedEdge() {
    OdbGraph graph = SimpleDomain.newGraph();

    Vertex v0 = graph.addVertex(T.label, TestNode.LABEL);
    Vertex v1 = graph.addVertex(T.label, TestNode.LABEL);

    v0.addEdge(TestEdge.LABEL, v1);

    Edge e0 = v0.edges(Direction.OUT, TestEdge.LABEL).next();
    e0.property(TestEdge.LONG_PROPERTY, 1L);
    assertEquals(1L, e0.property(TestEdge.LONG_PROPERTY).value());
  }

  @Test
  public void setAndGetEdgePropertyViaDifferenceQueriedEdges() {
    OdbGraph graph = SimpleDomain.newGraph();

    Vertex v0 = graph.addVertex(T.label, TestNode.LABEL);
    Vertex v1 = graph.addVertex(T.label, TestNode.LABEL);

    v0.addEdge(TestEdge.LABEL, v1);

    Edge e0ViaOut = v0.edges(Direction.OUT, TestEdge.LABEL).next();
    e0ViaOut.property(TestEdge.LONG_PROPERTY, 1L);

    Edge e0ViaIn = v1.edges(Direction.IN, TestEdge.LABEL).next();
    assertEquals(1L, e0ViaIn.property(TestEdge.LONG_PROPERTY).value());
  }

  @Test
  public void setAndGetEdgePropertyViaNewEdgeMultiple() {
    OdbGraph graph = SimpleDomain.newGraph();

    Vertex v0 = graph.addVertex(T.label, TestNode.LABEL);
    Vertex v1 = graph.addVertex(T.label, TestNode.LABEL);

    Edge e0 = v0.addEdge(TestEdge.LABEL, v1);
    Edge e1 = v0.addEdge(TestEdge.LABEL, v1);

    e0.property(TestEdge.LONG_PROPERTY, 1L);
    e1.property(TestEdge.LONG_PROPERTY, 2L);

    assertEquals(1L, e0.property(TestEdge.LONG_PROPERTY).value());
    assertEquals(2L, e1.property(TestEdge.LONG_PROPERTY).value());
  }

  @Test
  public void setAndGetEdgePropertyViaQueriedEdgeMultiple() {
    OdbGraph graph = SimpleDomain.newGraph();

    Vertex v0 = graph.addVertex(T.label, TestNode.LABEL);
    Vertex v1 = graph.addVertex(T.label, TestNode.LABEL);

    v0.addEdge(TestEdge.LABEL, v1);
    v0.addEdge(TestEdge.LABEL, v1);

    Iterator<Edge> edgeIt = v0.edges(Direction.OUT, TestEdge.LABEL);

    Edge e0 = edgeIt.next();
    Edge e1 = edgeIt.next();

    e0.property(TestEdge.LONG_PROPERTY, 1L);
    e1.property(TestEdge.LONG_PROPERTY, 2L);

    assertEquals(1L, e0.property(TestEdge.LONG_PROPERTY).value());
    assertEquals(2L, e1.property(TestEdge.LONG_PROPERTY).value());
  }

  @Test
  public void setAndGetEdgePropertyViaDifferenceQueriedEdgesMultiple() {
    OdbGraph graph = SimpleDomain.newGraph();

    Vertex v0 = graph.addVertex(T.label, TestNode.LABEL);
    Vertex v1 = graph.addVertex(T.label, TestNode.LABEL);

    v0.addEdge(TestEdge.LABEL, v1);
    v0.addEdge(TestEdge.LABEL, v1);

    Iterator<Edge> outEdgeIt = v0.edges(Direction.OUT, TestEdge.LABEL);
    Iterator<Edge> inEdgeIt = v1.edges(Direction.IN, TestEdge.LABEL);

    Edge e0ViaOut = outEdgeIt.next();
    Edge e1ViaOut = outEdgeIt.next();
    Edge e0ViaIn = inEdgeIt.next();
    Edge e1ViaIn = inEdgeIt.next();

    e0ViaOut.property(TestEdge.LONG_PROPERTY, 1L);
    e1ViaOut.property(TestEdge.LONG_PROPERTY, 2L);

    assertEquals(1L, e0ViaIn.property(TestEdge.LONG_PROPERTY).value());
    assertEquals(2L, e1ViaIn.property(TestEdge.LONG_PROPERTY).value());
  }

  @Test
  public void removeEdgeSimple() {
    try (OdbGraph graph = SimpleDomain.newGraph()) {
      Vertex v0 = graph.addVertex(T.label, TestNode.LABEL, TestNode.STRING_PROPERTY, "v0");
      Vertex v1 = graph.addVertex(T.label, TestNode.LABEL, TestNode.STRING_PROPERTY, "v1");
      Edge edge = v0.addEdge(TestEdge.LABEL, v1, TestEdge.LONG_PROPERTY, 1l);

      edge.remove();

      assertFalse(v0.edges(Direction.OUT).hasNext());
      assertFalse(v1.edges(Direction.IN).hasNext());
    }
  }

  @Test
  public void removeEdgeComplex1() {
    try (OdbGraph graph = SimpleDomain.newGraph()) {
      Vertex v0 = graph.addVertex(T.label, TestNode.LABEL, TestNode.STRING_PROPERTY, "v0");
      Vertex v1 = graph.addVertex(T.label, TestNode.LABEL, TestNode.STRING_PROPERTY, "v1");
      Edge edge0 = v0.addEdge(TestEdge.LABEL, v1, TestEdge.LONG_PROPERTY, 0l);
      Edge edge1 = v0.addEdge(TestEdge.LABEL, v1, TestEdge.LONG_PROPERTY, 1l);

      edge0.remove();

      Iterator<Edge> v0outEdges = v0.edges(Direction.OUT);
      assertEquals(Long.valueOf(1), v0outEdges.next().value(TestEdge.LONG_PROPERTY));
      assertFalse(v0outEdges.hasNext());
      Iterator<Edge> v1inEdges = v0.edges(Direction.OUT);
      assertEquals(Long.valueOf(1), v1inEdges.next().value(TestEdge.LONG_PROPERTY));
      assertFalse(v1inEdges.hasNext());
    }
  }

  @Test
  public void removeEdgeComplex2() {
    try (OdbGraph graph = SimpleDomain.newGraph()) {
      Vertex v0 = graph.addVertex(T.label, TestNode.LABEL, TestNode.STRING_PROPERTY, "v0");
      Vertex v1 = graph.addVertex(T.label, TestNode.LABEL, TestNode.STRING_PROPERTY, "v1");
      Edge edge0 = v0.addEdge(TestEdge.LABEL, v1, TestEdge.LONG_PROPERTY, 0l);
      Edge edge1 = v0.addEdge(TestEdge.LABEL, v1, TestEdge.LONG_PROPERTY, 1l);

      edge1.remove();

      Iterator<Edge> v0outEdges = v0.edges(Direction.OUT);
      assertEquals(Long.valueOf(0), v0outEdges.next().value(TestEdge.LONG_PROPERTY));
      assertFalse(v0outEdges.hasNext());
      Iterator<Edge> v1inEdges = v0.edges(Direction.OUT);
      assertEquals(Long.valueOf(0), v1inEdges.next().value(TestEdge.LONG_PROPERTY));
      assertFalse(v1inEdges.hasNext());
    }
  }

  @Test
  public void removeEdgeComplexAfterSerialization() {
    try (OdbGraph graph = SimpleDomain.newGraph()) {
      Vertex v0 = graph.addVertex(T.label, TestNode.LABEL, TestNode.STRING_PROPERTY, "v0");
      Vertex v1 = graph.addVertex(T.label, TestNode.LABEL, TestNode.STRING_PROPERTY, "v1");
      v0.addEdge(TestEdge.LABEL, v1, TestEdge.LONG_PROPERTY, 0l);
      v0.addEdge(TestEdge.LABEL, v1, TestEdge.LONG_PROPERTY, 1l);

      // round trip serialization, delete edge with longProperty=0;
      graph.referenceManager.clearAllReferences();
      graph.traversal().V(v0.id()).outE().has(TestEdge.LONG_PROPERTY, P.eq(0l)).drop().iterate();

      Iterator<Edge> v0outEdges = v0.edges(Direction.OUT);
      assertEquals(Long.valueOf(1), v0outEdges.next().value(TestEdge.LONG_PROPERTY));
      assertFalse(v0outEdges.hasNext());
      Iterator<Edge> v1inEdges = v1.edges(Direction.IN);
      assertEquals(Long.valueOf(1), v1inEdges.next().value(TestEdge.LONG_PROPERTY));
      assertFalse(v1inEdges.hasNext());
    }
  }

  @Test
  public void removeNodeSimple() {
    try (OdbGraph graph = SimpleDomain.newGraph()) {
      Vertex v0 = graph.addVertex(T.label, TestNode.LABEL, TestNode.STRING_PROPERTY, "v0");
      Vertex v1 = graph.addVertex(T.label, TestNode.LABEL, TestNode.STRING_PROPERTY, "v1");
      v0.addEdge(TestEdge.LABEL, v1, TestEdge.LONG_PROPERTY, 1l);

      v0.remove();

      assertEquals(Long.valueOf(1), graph.traversal().V().count().next());
      assertFalse(v1.edges(Direction.IN).hasNext());
    }
  }

  @Test
  public void shouldAllowAddingElementsAndSettingProperties() {
    try(OdbGraph graph = GratefulDead.newGraph()) {

      Vertex song1 = graph.addVertex(Song.label);
      Vertex song2 = graph.addVertex(Song.label);
      song1.property(Song.NAME, "song 1");
      song2.property(Song.NAME, "song 2");

      List<Vertex> vertices = graph.traversal().V().toList();
      assertEquals(2, vertices.size());
      Set<Object> names = graph.traversal().V().values("name").toSet();
      assertTrue(names.contains("song 1"));
      assertTrue(names.contains("song 2"));

      song1.addEdge(FollowedBy.LABEL, song2, FollowedBy.WEIGHT, new Integer(42));
      assertEquals(42, graph.traversal().E().values(FollowedBy.WEIGHT).next());
      assertEquals(42, __(song1).outE().values(FollowedBy.WEIGHT).next());
    }
  }

  @Test
  public void shouldSupportEdgeRemoval() {
    try (OdbGraph graph = GratefulDead.newGraph()) {
      Vertex song1 = graph.addVertex(Song.label);
      Vertex song2 = graph.addVertex(Song.label);
      Edge followedBy = song1.addEdge(FollowedBy.LABEL, song2);
      assertEquals(2, graph.traversal().V().toList().size());
      assertEquals(1, graph.traversal().E().toList().size());

      followedBy.remove();
      assertEquals(2, graph.traversal().V().toList().size());
      assertEquals(0, graph.traversal().E().toList().size());
    }
  }

  @Test
  public void shouldSupportVertexRemoval1() {
    try (OdbGraph graph = GratefulDead.newGraph()) {
      Vertex song1 = graph.addVertex(Song.label);
      Vertex song2 = graph.addVertex(Song.label);
      song1.addEdge(FollowedBy.LABEL, song2);
      assertEquals(2, graph.traversal().V().toList().size());
      assertEquals(1, graph.traversal().E().toList().size());

      song1.remove();
      assertEquals(1, graph.traversal().V().toList().size());
      assertEquals(0, graph.traversal().E().toList().size());

      song2.remove();
      assertEquals(0, graph.traversal().V().toList().size());
    }
  }

  @Test
  public void shouldSupportVertexRemoval2() {
    try (OdbGraph graph = GratefulDead.newGraph()) {
      Vertex song1 = graph.addVertex(Song.label);
      Vertex song2 = graph.addVertex(Song.label);
      song1.addEdge(FollowedBy.LABEL, song2);
      assertEquals(2, graph.traversal().V().toList().size());
      assertEquals(1, graph.traversal().E().toList().size());

      song2.remove();
      assertEquals(1, graph.traversal().V().toList().size());
      assertEquals(0, graph.traversal().E().toList().size());

      song1.remove();
      assertEquals(0, graph.traversal().V().toList().size());
    }
  }

  @Test
  public void shouldAllowToSpecifyIds() {
    try(OdbGraph graph = GratefulDead.newGraph()) {

      Vertex v10 = graph.addVertex(T.id, 10l, T.label, Song.label, Song.NAME, "Song 10");
      Vertex v20 = graph.addVertex(T.id,  20, T.label, Song.label, Song.NAME, "Song 20");
      v10.addEdge(FollowedBy.LABEL, v20, FollowedBy.WEIGHT, 5);

      assertEquals(5, graph.traversal().V(10l).outE(FollowedBy.LABEL).values(FollowedBy.WEIGHT).next());
      assertEquals(5, graph.traversal().V(20).inE(FollowedBy.LABEL).values(FollowedBy.WEIGHT).next());
    }
  }

  @Test
  public void shouldReturnElementRefs() {
    try (OdbGraph graph = GratefulDead.newGraph()) {
      Vertex v0 = graph.addVertex(T.label, Song.label, Song.NAME, "Song 1");
      Vertex v2 = graph.addVertex(T.label, Song.label, Song.NAME, "Song 2");
      Edge e4 = v0.addEdge(FollowedBy.LABEL, v2);
      assertTrue(v0 instanceof NodeRef);
      assertTrue(v0.vertices(Direction.OUT).next() instanceof NodeRef);
    }
  }

}
