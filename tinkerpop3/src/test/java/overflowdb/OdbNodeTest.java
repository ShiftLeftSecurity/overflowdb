package overflowdb;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import overflowdb.testdomains.gratefuldead.Artist;
import overflowdb.testdomains.gratefuldead.ArtistDb;
import overflowdb.testdomains.gratefuldead.FollowedBy;
import overflowdb.testdomains.gratefuldead.GratefulDead;
import overflowdb.testdomains.gratefuldead.Song;
import overflowdb.testdomains.simple.SimpleDomain;
import overflowdb.testdomains.simple.TestEdge;
import overflowdb.testdomains.simple.TestNode;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OdbNodeTest {

  @Test
  public void simpleTest() {
    try (OdbGraph graph = SimpleDomain.newGraph()) {
      Node n1 = graph.addNode(
          TestNode.LABEL,
          TestNode.STRING_PROPERTY, "node 1",
          TestNode.INT_PROPERTY, 42,
          TestNode.STRING_LIST_PROPERTY, Arrays.asList("stringOne", "stringTwo"),
          TestNode.INT_LIST_PROPERTY, Arrays.asList(42, 43));
      Node n2 = graph.addNode(
          TestNode.LABEL,
          TestNode.STRING_PROPERTY, "node 2",
          TestNode.INT_PROPERTY, 52,
          TestNode.STRING_LIST_PROPERTY, Arrays.asList("stringThree", "stringFour"),
          TestNode.INT_LIST_PROPERTY, Arrays.asList(52, 53));
      OdbEdgeTp3 e = n1.addEdge2(TestEdge.LABEL, n2, TestEdge.LONG_PROPERTY, 99l);

      //  verify that we can cast to our domain-specific nodes/edges
      TestNode node1 = (TestNode) n1;
      assertEquals("node 1", node1.stringProperty());
      assertEquals(Integer.valueOf(42), node1.intProperty());
      TestEdge testEdge = (TestEdge) e;
      assertEquals(Long.valueOf(99), testEdge.longProperty());

      //trim test
      assertEquals(2 + (((long)4)<<32), ((NodeRef)n2).get().trim());
      assertEquals(2 + (((long)2)<<32), ((NodeRef)n2).get().trim());
      assertEquals(2 + (((long)4)<<32), ((NodeRef)n1).get().trim());
      assertEquals(2 + (((long)2)<<32), ((NodeRef)n1).get().trim());

      // node traversals
      assertSize(1, n1.out());
      assertSize(0, n1.out("otherLabel"));
      assertSize(0, n2.out());
      assertSize(0, n1.in());
      assertSize(1, n2.in());
      assertSize(1, n1.both());
      assertSize(1, n2.both());

      // edge traversals
      assertSize(1, n1.outE());
      assertEquals(TestEdge.LABEL, n1.outE().next().label());
      assertSize(0, n1.outE("otherLabel"));
      assertSize(0, n2.outE());
      assertSize(1, n2.inE());
      assertSize(1, n1.bothE());
      assertSize(1, n1.bothE(TestEdge.LABEL));
      assertSize(0, n1.bothE("otherLabel"));

      // node properties
      // TODO move to tinkerpop-subproject once it's factored out
      Set stringProperties = graph.traversal().V().values(TestNode.STRING_PROPERTY).toSet();
      assertTrue(stringProperties.contains("node 1"));
      assertTrue(stringProperties.contains("node 2"));
      assertEquals(42, e.outNode().property2(TestNode.INT_PROPERTY));
      assertEquals(52, e.inNode().property2(TestNode.INT_PROPERTY));

      // edge properties
      assertTrue(e instanceof TestEdge);
      assertEquals(Long.valueOf(99l), ((TestEdge) e).longProperty());
      assertEquals(Long.valueOf(99l), e.value(TestEdge.LONG_PROPERTY));
      assertEquals(99l, (long) n1.outE().next().property2(TestEdge.LONG_PROPERTY));
      assertEquals(99l, (long) n2.inE().next().property2(TestEdge.LONG_PROPERTY));
      assertEquals(new HashMap<String, Object>() {{ put(TestEdge.LONG_PROPERTY, 99l); }}, n2.inE().next().propertyMap());
    }
  }

  @Test
  public void loadGratefulDeadGraph() throws IOException {
    try(OdbGraph graph = GratefulDead.openAndLoadSampleData()) {
      final Node node1 = graph.node(1);
      assertEquals("HEY BO DIDDLEY", node1.property2("name"));

      // TODO move to tinkerpop-subproject once it's factored out
      List<Vertex> garcias = graph.traversal().V().has("name", "Garcia").toList();
      assertEquals(garcias.size(), 1);
      Artist artist = (Artist) garcias.get(0);
      ArtistDb garcia = artist.get();
      assertEquals("Garcia", garcia.name());
    }
  }

  @Test
  public void testEdgeEquality() {
    OdbGraph graph = SimpleDomain.newGraph();

    Node n0 = graph.addNode(TestNode.LABEL);
    Node n1 = graph.addNode(TestNode.LABEL);

    OdbEdgeTp3 e0 = n0.addEdge2(TestEdge.LABEL, n1, TestEdge.LONG_PROPERTY, 99l);

    OdbEdgeTp3 e0FromOut = n0.outE().next();
    OdbEdgeTp3 e0FromIn = n1.inE().next();

    assertEquals(e0, e0FromOut);
    assertEquals(e0, e0FromIn);
    assertEquals(e0FromOut, e0FromIn);
  }

  @Test
  public void setAndGetEdgePropertyViaNewEdge() {
    OdbGraph graph = SimpleDomain.newGraph();

    Node n0 = graph.addNode(TestNode.LABEL);
    Node n1 = graph.addNode(TestNode.LABEL);

    OdbEdgeTp3 e0 = n0.addEdge2(TestEdge.LABEL, n1, TestEdge.LONG_PROPERTY, 1L);
    assertEquals(1L, (long) e0.property2(TestEdge.LONG_PROPERTY));

    OdbEdgeTp3 e1 = n0.addEdge2(TestEdge.LABEL, n1);
    e1.setProperty(TestEdge.LONG_PROPERTY, 2L);
    assertEquals(2L, (long) e1.property2(TestEdge.LONG_PROPERTY));
  }

  @Test
  public void setAndGetEdgePropertyViaQueriedEdge() {
    OdbGraph graph = SimpleDomain.newGraph();

    Node n0 = graph.addNode(TestNode.LABEL);
    Node n1 = graph.addNode(TestNode.LABEL);

    n0.addEdge2(TestEdge.LABEL, n1);

    OdbEdgeTp3 e0 = n0.outE(TestEdge.LABEL).next();
    e0.setProperty(TestEdge.LONG_PROPERTY, 1L);
    assertEquals(1L, (long) e0.property2(TestEdge.LONG_PROPERTY));
  }

  @Test
  public void setAndGetEdgePropertyViaDifferenceQueriedEdges() {
    OdbGraph graph = SimpleDomain.newGraph();

    Node n0 = graph.addNode(TestNode.LABEL);
    Node n1 = graph.addNode(TestNode.LABEL);

    n0.addEdge2(TestEdge.LABEL, n1);

    OdbEdgeTp3 e0ViaOut = n0.outE(TestEdge.LABEL).next();
    e0ViaOut.setProperty(TestEdge.LONG_PROPERTY, 1L);

    OdbEdgeTp3 e0ViaIn = n1.inE(TestEdge.LABEL).next();
    assertEquals(1L, (long) e0ViaIn.property2(TestEdge.LONG_PROPERTY));
  }

  @Test
  public void setAndGetEdgePropertyViaNewEdgeMultiple() {
    OdbGraph graph = SimpleDomain.newGraph();

    Node n0 = graph.addNode(TestNode.LABEL);
    Node n1 = graph.addNode(TestNode.LABEL);

    OdbEdgeTp3 e0 = n0.addEdge2(TestEdge.LABEL, n1);
    OdbEdgeTp3 e1 = n0.addEdge2(TestEdge.LABEL, n1);

    e0.setProperty(TestEdge.LONG_PROPERTY, 1L);
    e1.setProperty(TestEdge.LONG_PROPERTY, 2L);

    assertEquals(1L, (long) e0.property2(TestEdge.LONG_PROPERTY));
    assertEquals(2L, (long) e1.property2(TestEdge.LONG_PROPERTY));
  }

  @Test
  public void setAndGetEdgePropertyViaQueriedEdgeMultiple() {
    OdbGraph graph = SimpleDomain.newGraph();

    Node n0 = graph.addNode(TestNode.LABEL);
    Node n1 = graph.addNode(TestNode.LABEL);

    n0.addEdge2(TestEdge.LABEL, n1);
    n0.addEdge2(TestEdge.LABEL, n1);

    Iterator<OdbEdgeTp3> edgeIt = n0.outE(TestEdge.LABEL);

    OdbEdgeTp3 e0 = edgeIt.next();
    OdbEdgeTp3 e1 = edgeIt.next();

    e0.setProperty(TestEdge.LONG_PROPERTY, 1L);
    e1.setProperty(TestEdge.LONG_PROPERTY, 2L);

    assertEquals(1L, (long) e0.property2(TestEdge.LONG_PROPERTY));
    assertEquals(2L, (long) e1.property2(TestEdge.LONG_PROPERTY));
  }

  @Test
  public void setAndGetEdgePropertyViaDifferenceQueriedEdgesMultiple() {
    OdbGraph graph = SimpleDomain.newGraph();

    Node n0 = graph.addNode(TestNode.LABEL);
    Node n1 = graph.addNode(TestNode.LABEL);

    n0.addEdge2(TestEdge.LABEL, n1);
    n0.addEdge2(TestEdge.LABEL, n1);

    Iterator<OdbEdgeTp3> outEdgeIt = n0.outE(TestEdge.LABEL);
    Iterator<OdbEdgeTp3> inEdgeIt = n1.inE(TestEdge.LABEL);

    OdbEdgeTp3 e0ViaOut = outEdgeIt.next();
    OdbEdgeTp3 e1ViaOut = outEdgeIt.next();
    OdbEdgeTp3 e0ViaIn = inEdgeIt.next();
    OdbEdgeTp3 e1ViaIn = inEdgeIt.next();

    e0ViaOut.setProperty(TestEdge.LONG_PROPERTY, 1L);
    e1ViaOut.setProperty(TestEdge.LONG_PROPERTY, 2L);

    assertEquals(1L, (long) e0ViaIn.property2(TestEdge.LONG_PROPERTY));
    assertEquals(2L, (long) e1ViaIn.property2(TestEdge.LONG_PROPERTY));
  }

  @Test
  public void removeEdgeSimple() {
    try (OdbGraph graph = SimpleDomain.newGraph()) {
      Node n0 = graph.addNode(TestNode.LABEL, TestNode.STRING_PROPERTY, "n0");
      Node n1 = graph.addNode(TestNode.LABEL, TestNode.STRING_PROPERTY, "n1");
      OdbEdgeTp3 edge = n0.addEdge2(TestEdge.LABEL, n1, TestEdge.LONG_PROPERTY, 1l);

      edge.remove();

      assertFalse(n0.outE().hasNext());
      assertFalse(n1.inE().hasNext());
    }
  }

  @Test
  public void removeEdgeComplex1() {
    try (OdbGraph graph = SimpleDomain.newGraph()) {
      Node n0 = graph.addNode(TestNode.LABEL, TestNode.STRING_PROPERTY, "n0");
      Node n1 = graph.addNode(TestNode.LABEL, TestNode.STRING_PROPERTY, "n1");
      OdbEdgeTp3 edge0 = n0.addEdge2(TestEdge.LABEL, n1, TestEdge.LONG_PROPERTY, 0l);
      OdbEdgeTp3 edge1 = n0.addEdge2(TestEdge.LABEL, n1, TestEdge.LONG_PROPERTY, 1l);

      edge0.remove();

      Iterator<OdbEdgeTp3> n0outEdges = n0.outE();
      assertEquals(1l, (long) n0outEdges.next().property2(TestEdge.LONG_PROPERTY));
      assertFalse(n0outEdges.hasNext());
      Iterator<OdbEdgeTp3> n1inEdges = n1.inE();
      assertEquals(1l, (long) n1inEdges.next().property2(TestEdge.LONG_PROPERTY));
      assertFalse(n1inEdges.hasNext());
    }
  }

  @Test
  public void removeEdgeComplex2() {
    try (OdbGraph graph = SimpleDomain.newGraph()) {
      Node n0 = graph.addNode(TestNode.LABEL, TestNode.STRING_PROPERTY, "n0");
      Node n1 = graph.addNode(TestNode.LABEL, TestNode.STRING_PROPERTY, "n1");
      OdbEdgeTp3 edge0 = n0.addEdge2(TestEdge.LABEL, n1, TestEdge.LONG_PROPERTY, 0l);
      OdbEdgeTp3 edge1 = n0.addEdge2(TestEdge.LABEL, n1, TestEdge.LONG_PROPERTY, 1l);

      edge1.remove();

      Iterator<OdbEdgeTp3> n0outEdges = n0.outE();
      assertEquals(Long.valueOf(0), n0outEdges.next().value(TestEdge.LONG_PROPERTY));
      assertFalse(n0outEdges.hasNext());
      Iterator<OdbEdgeTp3> n1inEdges = n0.outE();
      assertEquals(Long.valueOf(0), n1inEdges.next().value(TestEdge.LONG_PROPERTY));
      assertFalse(n1inEdges.hasNext());
    }
  }

  @Test
  public void removeEdgeComplexAfterSerialization() {
    try (OdbGraph graph = SimpleDomain.newGraph()) {
      Node n0 = graph.addNode(TestNode.LABEL, TestNode.STRING_PROPERTY, "n0");
      Node n1 = graph.addNode(TestNode.LABEL, TestNode.STRING_PROPERTY, "n1");
      n0.addEdge2(TestEdge.LABEL, n1, TestEdge.LONG_PROPERTY, 0l);
      n0.addEdge2(TestEdge.LABEL, n1, TestEdge.LONG_PROPERTY, 1l);

      // round trip serialization, delete edge with longProperty=0;
      graph.referenceManager.clearAllReferences();
      // TODO move to tinkerpop-subproject once it's factored out
      graph.traversal().V(n0.id()).outE().has(TestEdge.LONG_PROPERTY, P.eq(0l)).drop().iterate();

      Iterator<OdbEdgeTp3> n0outEdges = n0.outE();
      assertEquals(Long.valueOf(1), n0outEdges.next().value(TestEdge.LONG_PROPERTY));
      assertFalse(n0outEdges.hasNext());
      Iterator<OdbEdgeTp3> n1inEdges = n1.inE();
      assertEquals(Long.valueOf(1), n1inEdges.next().value(TestEdge.LONG_PROPERTY));
      assertFalse(n1inEdges.hasNext());
    }
  }

  @Test
  public void removeNodeSimple() {
    try (OdbGraph graph = SimpleDomain.newGraph()) {
      Node n0 = graph.addNode(TestNode.LABEL, TestNode.STRING_PROPERTY, "n0");
      Node n1 = graph.addNode(TestNode.LABEL, TestNode.STRING_PROPERTY, "n1");
      n0.addEdge2(TestEdge.LABEL, n1, TestEdge.LONG_PROPERTY, 1l);

      n0.remove();

      assertEquals(1, graph.nodeCount());
      assertFalse(n1.inE().hasNext());
    }
  }

  @Test
  public void shouldAllowAddingElementsAndSettingProperties() {
    try(OdbGraph graph = GratefulDead.open()) {

      Node song1 = graph.addNode(Song.label);
      Node song2 = graph.addNode(Song.label);
      song1.setProperty(Song.NAME, "song 1");
      song2.setProperty(Song.NAME, "song 2");

      assertNodeCount(2, graph);
      // TODO move to tinkerpop-subproject once it's factored out
      Set<Object> names = graph.traversal().V().values("name").toSet();
      assertTrue(names.contains("song 1"));
      assertTrue(names.contains("song 2"));

      song1.addEdge2(FollowedBy.LABEL, song2, FollowedBy.WEIGHT, new Integer(42));
      // TODO move to tinkerpop-subproject once it's factored out
      assertEquals(42, graph.traversal().E().values(FollowedBy.WEIGHT).next());
      assertEquals(42, (int) song1.outE().next().property2(FollowedBy.WEIGHT));
    }
  }

  @Test
  public void shouldSupportEdgeRemoval1() {
    try (OdbGraph graph = GratefulDead.open()) {
      Node song1 = graph.addNode(Song.label);
      Node song2 = graph.addNode(Song.label);
      OdbEdgeTp3 followedBy = song1.addEdge2(FollowedBy.LABEL, song2);
      assertNodeCount(2, graph);
      assertEdgeCount(1, graph);

      followedBy.remove();
      assertNodeCount(2, graph);
      assertEdgeCount(0, graph);
    }
  }

  @Test
  public void shouldSupportEdgeRemoval2() {
    try (OdbGraph graph = GratefulDead.open()) {
      Node song1 = graph.addNode(Song.label);
      Node song2 = graph.addNode(Song.label);
      Node song3 = graph.addNode(Song.label);
      OdbEdgeTp3 edge1 = song1.addEdge2(FollowedBy.LABEL, song2);
      OdbEdgeTp3 edge2 = song1.addEdge2(FollowedBy.LABEL, song3);
      assertNodeCount(3, graph);
      assertEdgeCount(2, graph);

      edge1.remove();
      assertNodeCount(3, graph);
      assertEdgeCount(1, graph);

      edge2.remove();
      assertNodeCount(3, graph);
      assertEdgeCount(0, graph);
    }
  }

  @Test
  public void nodeRemove1() {
    try (OdbGraph graph = GratefulDead.open()) {
      Node song1 = graph.addNode(Song.label);
      Node song2 = graph.addNode(Song.label);
      song1.addEdge2(FollowedBy.LABEL, song2);
      assertNodeCount(2, graph);
      assertEdgeCount(1, graph);

      song1.remove();
      assertNodeCount(1, graph);
      assertEdgeCount(0, graph);

      song2.remove();
      assertNodeCount(0, graph);
    }
  }

  @Test
  public void nodeRemove2() {
    try (OdbGraph graph = GratefulDead.open()) {
      Node song1 = graph.addNode(Song.label);
      Node song2 = graph.addNode(Song.label);
      song1.addEdge2(FollowedBy.LABEL, song2);
      assertNodeCount(2, graph);
      assertEdgeCount(1, graph);

      song2.remove();
      assertNodeCount(1, graph);
      assertEdgeCount(0, graph);

      song1.remove();
      assertNodeCount(0, graph);
    }
  }

  @Test
  public void nodeRemove3() {
    try (OdbGraph graph = GratefulDead.open()) {
      Node song1 = graph.addNode(Song.label);
      Node song2 = graph.addNode(Song.label);
      Node song3 = graph.addNode(Song.label);
      song1.addEdge2(FollowedBy.LABEL, song2);
      song1.addEdge2(FollowedBy.LABEL, song3);
      assertNodeCount(3, graph);
      assertEdgeCount(2, graph);

      song3.remove();
      song2.remove();
      assertNodeCount(1, graph);
      assertEdgeCount(0, graph);
    }
  }

  @Test
  public void nodeRemove4() {
    try (OdbGraph graph = GratefulDead.open()) {
      Node song1 = graph.addNode(Song.label);
      Node song2 = graph.addNode(Song.label);
      Node song3 = graph.addNode(Song.label);
      song1.addEdge2(FollowedBy.LABEL, song2);
      song1.addEdge2(FollowedBy.LABEL, song3);
      assertNodeCount(3, graph);
      assertEdgeCount(2, graph);

      song2.remove();
      song3.remove();
      assertNodeCount(1, graph);
      assertEdgeCount(0, graph);
    }
  }

  @Test
  public void nodeRemove5_twoEdgesBetweenTwoNodes() {
    try (OdbGraph graph = GratefulDead.open()) {
      Node song1 = graph.addNode(Song.label);
      Node song2 = graph.addNode(Song.label);
      song1.addEdge2(FollowedBy.LABEL, song2);
      song1.addEdge2(FollowedBy.LABEL, song2);
      assertNodeCount(2, graph);
      assertEdgeCount(2, graph);

      song2.remove();
      assertNodeCount(1, graph);
      assertEdgeCount(0, graph);
    }
  }

  @Test
  public void nodeRemove6() {
    try (OdbGraph graph = GratefulDead.open()) {
      Node song1 = graph.addNode(Song.label);
      Node song2 = graph.addNode(Song.label);
      Node song3 = graph.addNode(Song.label);
      song1.addEdge2(FollowedBy.LABEL, song2);
      song1.addEdge2(FollowedBy.LABEL, song2);
      song2.addEdge2(FollowedBy.LABEL, song3);
      song2.addEdge2(FollowedBy.LABEL, song3);
      song3.addEdge2(FollowedBy.LABEL, song1);
      song3.addEdge2(FollowedBy.LABEL, song1);

      song2.remove();
      assertNodeCount(2, graph);
      assertEdgeCount(2, graph);

      assertSize(2, graph.nodes(Song.label));
      // TODO move to tinkerpop-subproject once it's factored out
      assertEquals(new Long(2), graph.traversal().E().hasLabel(FollowedBy.LABEL).count().next());
    }
  }

  @Test
  public void shouldAllowToSpecifyIds() {
    try(OdbGraph graph = GratefulDead.open()) {
      Node n10 = graph.addNode(10l, Song.label, Song.NAME, "Song 10");
      Node n20 = graph.addNode(20l, Song.label, Song.NAME, "Song 20");
      n10.addEdge2(FollowedBy.LABEL, n20, FollowedBy.WEIGHT, 5);

      assertEquals(5, (int) graph.node(10).outE(FollowedBy.LABEL).next().property2(FollowedBy.WEIGHT));
      assertEquals(5, (int) graph.node(20).inE(FollowedBy.LABEL).next().property2(FollowedBy.WEIGHT));
    }
  }

  @Test
  public void shouldReturnElementRefs() {
    try (OdbGraph graph = GratefulDead.open()) {
      Node n0 = graph.addNode(Song.label, Song.NAME, "Song 1");
      Node n2 = graph.addNode(Song.label, Song.NAME, "Song 2");
      OdbEdgeTp3 e4 = n0.addEdge2(FollowedBy.LABEL, n2);
      assertTrue(n0 instanceof NodeRef);
      assertTrue(n0.out().next() instanceof NodeRef);
    }
  }

  private void assertNodeCount(int expected, OdbGraph graph) {
    assertEquals("node count different to expected", expected, graph.nodeCount());
  }

  private void assertEdgeCount(int expected, OdbGraph graph) {
    assertEquals("edge count different to expected", expected, graph.traversal().E().toList().size());
  }

  private void assertSize(int expected, Iterator iter) {
    int size = 0;
    while (iter.hasNext()) {
      size++;
      iter.next();
    }
    assertEquals("iterator size different to expected", expected, size);
  }

}
