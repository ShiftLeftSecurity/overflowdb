package overflowdb;

import org.junit.Test;
import overflowdb.testdomains.gratefuldead.FollowedBy;
import overflowdb.testdomains.gratefuldead.GratefulDead;
import overflowdb.testdomains.gratefuldead.Song;
import overflowdb.testdomains.simple.SimpleDomain;
import overflowdb.testdomains.simple.TestEdge;
import overflowdb.testdomains.simple.TestNode;
import overflowdb.util.IteratorUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ElementTest {

  @Test
  public void overviewTest() {
    try (Graph graph = SimpleDomain.newGraph()) {
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
      Edge e = n1.addEdge(TestEdge.LABEL, n2, TestEdge.LONG_PROPERTY, 99l);

      //  verify that we can cast to our domain-specific nodes/edges
      TestNode node1 = (TestNode) n1;
      assertEquals("node 1", node1.stringProperty());
      assertEquals("node 1", node1.property(TestNode.STRING_PROPERTY));
      assertEquals("node 1", node1.property(new PropertyKey<>(TestNode.STRING_PROPERTY)));
      assertEquals(Optional.of("node 1"), node1.propertyOption(TestNode.STRING_PROPERTY));
      assertEquals(Optional.of("node 1"), node1.propertyOption(new PropertyKey<>(TestNode.STRING_PROPERTY)));
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
      Set stringProperties = new HashSet();
      graph.nodes().forEachRemaining(node -> stringProperties.add(node.property(TestNode.STRING_PROPERTY)));
      assertTrue(stringProperties.contains("node 1"));
      assertTrue(stringProperties.contains("node 2"));
      assertEquals(42, e.outNode().property(TestNode.INT_PROPERTY));
      assertEquals(52, e.inNode().property(TestNode.INT_PROPERTY));

      // edge properties
      assertTrue(e instanceof TestEdge);
      assertEquals(Long.valueOf(99l), ((TestEdge) e).longProperty());
      assertEquals(Long.valueOf(99l), e.property(TestEdge.LONG_PROPERTY));
      assertEquals(99l, (long) n1.outE().next().property(TestEdge.LONG_PROPERTY));
      assertEquals(99l, (long) n2.inE().next().property(TestEdge.LONG_PROPERTY));
      assertEquals(new HashMap<String, Object>() {{ put(TestEdge.LONG_PROPERTY, 99l); }}, n2.inE().next().propertyMap());
    }
  }

  @Test
  public void testEdgeEquality() {
    Graph graph = SimpleDomain.newGraph();

    Node n0 = graph.addNode(TestNode.LABEL);
    Node n1 = graph.addNode(TestNode.LABEL);

    Edge e0 = n0.addEdge(TestEdge.LABEL, n1, TestEdge.LONG_PROPERTY, 99l);

    Edge e0FromOut = n0.outE().next();
    Edge e0FromIn = n1.inE().next();

    assertEquals(e0, e0FromOut);
    assertEquals(e0, e0FromIn);
    assertEquals(e0FromOut, e0FromIn);
  }

  @Test
  public void setAndGetEdgePropertyViaNewEdge() {
    Graph graph = SimpleDomain.newGraph();

    Node n0 = graph.addNode(TestNode.LABEL);
    Node n1 = graph.addNode(TestNode.LABEL);

    Edge e0 = n0.addEdge(TestEdge.LABEL, n1, TestEdge.LONG_PROPERTY, 1L);
    assertEquals(1L, (long) e0.property(TestEdge.LONG_PROPERTY));

    Edge e1 = n0.addEdge(TestEdge.LABEL, n1);
    e1.setProperty(TestEdge.LONG_PROPERTY, 2L);
    assertEquals(2L, (long) e1.property(TestEdge.LONG_PROPERTY));
  }

  @Test
  public void setAndGetEdgePropertyViaQueriedEdge() {
    Graph graph = SimpleDomain.newGraph();

    Node n0 = graph.addNode(TestNode.LABEL);
    Node n1 = graph.addNode(TestNode.LABEL);

    n0.addEdge(TestEdge.LABEL, n1);

    Edge e0 = n0.outE(TestEdge.LABEL).next();
    e0.setProperty(TestEdge.LONG_PROPERTY, 1L);
    assertEquals(1L, (long) e0.property(TestEdge.LONG_PROPERTY));
  }

  @Test
  public void setAndGetEdgePropertyViaDifferenceQueriedEdges() {
    Graph graph = SimpleDomain.newGraph();

    Node n0 = graph.addNode(TestNode.LABEL);
    Node n1 = graph.addNode(TestNode.LABEL);

    n0.addEdge(TestEdge.LABEL, n1);

    Edge e0ViaOut = n0.outE(TestEdge.LABEL).next();
    e0ViaOut.setProperty(TestEdge.LONG_PROPERTY, 1L);

    Edge e0ViaIn = n1.inE(TestEdge.LABEL).next();
    assertEquals(1L, (long) e0ViaIn.property(TestEdge.LONG_PROPERTY));
  }

  @Test
  public void setAndGetEdgePropertyViaNewEdgeMultiple() {
    Graph graph = SimpleDomain.newGraph();

    Node n0 = graph.addNode(TestNode.LABEL);
    Node n1 = graph.addNode(TestNode.LABEL);

    Edge e0 = n0.addEdge(TestEdge.LABEL, n1);
    Edge e1 = n0.addEdge(TestEdge.LABEL, n1);

    e0.setProperty(TestEdge.LONG_PROPERTY, 1L);
    e1.setProperty(TestEdge.LONG_PROPERTY, 2L);

    assertEquals(1L, (long) e0.property(TestEdge.LONG_PROPERTY));
    assertEquals(2L, (long) e1.property(TestEdge.LONG_PROPERTY));
  }

  @Test
  public void setAndGetEdgePropertyViaQueriedEdgeMultiple() {
    Graph graph = SimpleDomain.newGraph();

    Node n0 = graph.addNode(TestNode.LABEL);
    Node n1 = graph.addNode(TestNode.LABEL);

    n0.addEdge(TestEdge.LABEL, n1);
    n0.addEdge(TestEdge.LABEL, n1);

    Iterator<Edge> edgeIt = n0.outE(TestEdge.LABEL);

    Edge e0 = edgeIt.next();
    Edge e1 = edgeIt.next();

    e0.setProperty(TestEdge.LONG_PROPERTY, 1L);
    e1.setProperty(TestEdge.LONG_PROPERTY, 2L);

    assertEquals(1L, (long) e0.property(TestEdge.LONG_PROPERTY));
    assertEquals(2L, (long) e1.property(TestEdge.LONG_PROPERTY));
  }

  @Test
  public void setAndGetEdgePropertyViaDifferenceQueriedEdgesMultiple() {
    Graph graph = SimpleDomain.newGraph();

    Node n0 = graph.addNode(TestNode.LABEL);
    Node n1 = graph.addNode(TestNode.LABEL);

    n0.addEdge(TestEdge.LABEL, n1);
    n0.addEdge(TestEdge.LABEL, n1);

    Iterator<Edge> outEdgeIt = n0.outE(TestEdge.LABEL);
    Iterator<Edge> inEdgeIt = n1.inE(TestEdge.LABEL);

    Edge e0ViaOut = outEdgeIt.next();
    Edge e1ViaOut = outEdgeIt.next();
    Edge e0ViaIn = inEdgeIt.next();
    Edge e1ViaIn = inEdgeIt.next();

    e0ViaOut.setProperty(TestEdge.LONG_PROPERTY, 1L);
    e1ViaOut.setProperty(TestEdge.LONG_PROPERTY, 2L);

    assertEquals(1L, (long) e0ViaIn.property(TestEdge.LONG_PROPERTY));
    assertEquals(2L, (long) e1ViaIn.property(TestEdge.LONG_PROPERTY));
  }

  @Test
  public void removeEdgeSimple() {
    try (Graph graph = SimpleDomain.newGraph()) {
      Node n0 = graph.addNode(TestNode.LABEL, TestNode.STRING_PROPERTY, "n0");
      Node n1 = graph.addNode(TestNode.LABEL, TestNode.STRING_PROPERTY, "n1");
      Edge edge = n0.addEdge(TestEdge.LABEL, n1, TestEdge.LONG_PROPERTY, 1l);

      edge.remove();

      assertFalse(n0.outE().hasNext());
      assertFalse(n1.inE().hasNext());
    }
  }

  @Test
  public void removeEdgeComplex1() {
    try (Graph graph = SimpleDomain.newGraph()) {
      Node n0 = graph.addNode(TestNode.LABEL, TestNode.STRING_PROPERTY, "n0");
      Node n1 = graph.addNode(TestNode.LABEL, TestNode.STRING_PROPERTY, "n1");
      Edge edge0 = n0.addEdge(TestEdge.LABEL, n1, TestEdge.LONG_PROPERTY, 0l);
      Edge edge1 = n0.addEdge(TestEdge.LABEL, n1, TestEdge.LONG_PROPERTY, 1l);

      edge0.remove();

      Iterator<Edge> n0outEdges = n0.outE();
      assertEquals(1l, (long) n0outEdges.next().property(TestEdge.LONG_PROPERTY));
      assertFalse(n0outEdges.hasNext());
      Iterator<Edge> n1inEdges = n1.inE();
      assertEquals(1l, (long) n1inEdges.next().property(TestEdge.LONG_PROPERTY));
      assertFalse(n1inEdges.hasNext());
    }
  }

  @Test
  public void removeEdgeComplex2() {
    try (Graph graph = SimpleDomain.newGraph()) {
      Node n0 = graph.addNode(TestNode.LABEL, TestNode.STRING_PROPERTY, "n0");
      Node n1 = graph.addNode(TestNode.LABEL, TestNode.STRING_PROPERTY, "n1");
      Edge edge0 = n0.addEdge(TestEdge.LABEL, n1, TestEdge.LONG_PROPERTY, 0l);
      Edge edge1 = n0.addEdge(TestEdge.LABEL, n1, TestEdge.LONG_PROPERTY, 1l);

      edge1.remove();

      Iterator<Edge> n0outEdges = n0.outE();
      assertEquals(Long.valueOf(0), n0outEdges.next().property(TestEdge.LONG_PROPERTY));
      assertFalse(n0outEdges.hasNext());
      Iterator<Edge> n1inEdges = n0.outE();
      assertEquals(Long.valueOf(0), n1inEdges.next().property(TestEdge.LONG_PROPERTY));
      assertFalse(n1inEdges.hasNext());
    }
  }

  @Test
  public void removeEdgeComplexAfterSerialization() {
    try (Graph graph = SimpleDomain.newGraph()) {
      Node n0 = graph.addNode(TestNode.LABEL, TestNode.STRING_PROPERTY, "n0");
      Node n1 = graph.addNode(TestNode.LABEL, TestNode.STRING_PROPERTY, "n1");
      n0.addEdge(TestEdge.LABEL, n1, TestEdge.LONG_PROPERTY, 0l);
      n0.addEdge(TestEdge.LABEL, n1, TestEdge.LONG_PROPERTY, 1l);

      // round trip serialization, delete edge with longProperty=0;
      graph.referenceManager.clearAllReferences();
      graph.node(n0.id()).outE().forEachRemaining(edge -> {
        if (0L == (long) edge.property(TestEdge.LONG_PROPERTY)) {
          edge.remove();
        }
      });

      Iterator<Edge> n0outEdges = n0.outE();
      assertEquals(Long.valueOf(1), n0outEdges.next().property(TestEdge.LONG_PROPERTY));
      assertFalse(n0outEdges.hasNext());
      Iterator<Edge> n1inEdges = n1.inE();
      assertEquals(Long.valueOf(1), n1inEdges.next().property(TestEdge.LONG_PROPERTY));
      assertFalse(n1inEdges.hasNext());
    }
  }

  @Test
  public void removeNodeSimple() {
    try (Graph graph = SimpleDomain.newGraph()) {
      Node n0 = graph.addNode(TestNode.LABEL, TestNode.STRING_PROPERTY, "n0");
      Node n1 = graph.addNode(TestNode.LABEL, TestNode.STRING_PROPERTY, "n1");
      n0.addEdge(TestEdge.LABEL, n1, TestEdge.LONG_PROPERTY, 1l);

      n0.remove();

      assertEquals(1, graph.nodeCount());
      assertFalse(n1.inE().hasNext());
    }
  }

  @Test
  public void shouldAllowAddingElementsAndSettingProperties() {
    try(Graph graph = GratefulDead.newGraph()) {

      Node song1 = graph.addNode(Song.label);
      Node song2 = graph.addNode(Song.label);
      song1.setProperty(Song.NAME, "song 1");
      song2.setProperty(Song.NAME, "song 2");

      assertNodeCount(2, graph);
      Set names = new HashSet<>();
      graph.nodes().forEachRemaining(node -> names.add(node.property("name")));
      assertTrue(names.contains("song 1"));
      assertTrue(names.contains("song 2"));

      song1.addEdge(FollowedBy.LABEL, song2, FollowedBy.WEIGHT, new Integer(42));
      Iterator<Edge> edgesWithWeight = IteratorUtils.filter(graph.edges(), edge -> edge.property(FollowedBy.WEIGHT) != null);
      assertEquals(42, (int) edgesWithWeight.next().property(FollowedBy.WEIGHT));
      assertEquals(42, (int) song1.outE().next().property(FollowedBy.WEIGHT));
    }
  }

  @Test
  public void shouldSupportEdgeRemoval1() {
    try (Graph graph = GratefulDead.newGraph()) {
      Node song1 = graph.addNode(Song.label);
      Node song2 = graph.addNode(Song.label);
      Edge followedBy = song1.addEdge(FollowedBy.LABEL, song2);
      assertNodeCount(2, graph);
      assertEdgeCount(1, graph);

      followedBy.remove();
      assertNodeCount(2, graph);
      assertEdgeCount(0, graph);
    }
  }

  @Test
  public void shouldSupportEdgeRemoval2() {
    try (Graph graph = GratefulDead.newGraph()) {
      Node song1 = graph.addNode(Song.label);
      Node song2 = graph.addNode(Song.label);
      Node song3 = graph.addNode(Song.label);
      Edge edge1 = song1.addEdge(FollowedBy.LABEL, song2);
      Edge edge2 = song1.addEdge(FollowedBy.LABEL, song3);
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
    try (Graph graph = GratefulDead.newGraph()) {
      Node song1 = graph.addNode(Song.label);
      Node song2 = graph.addNode(Song.label);
      song1.addEdge(FollowedBy.LABEL, song2);
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
    try (Graph graph = GratefulDead.newGraph()) {
      Node song1 = graph.addNode(Song.label);
      Node song2 = graph.addNode(Song.label);
      song1.addEdge(FollowedBy.LABEL, song2);
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
    try (Graph graph = GratefulDead.newGraph()) {
      Node song1 = graph.addNode(Song.label);
      Node song2 = graph.addNode(Song.label);
      Node song3 = graph.addNode(Song.label);
      song1.addEdge(FollowedBy.LABEL, song2);
      song1.addEdge(FollowedBy.LABEL, song3);
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
    try (Graph graph = GratefulDead.newGraph()) {
      Node song1 = graph.addNode(Song.label);
      Node song2 = graph.addNode(Song.label);
      Node song3 = graph.addNode(Song.label);
      song1.addEdge(FollowedBy.LABEL, song2);
      song1.addEdge(FollowedBy.LABEL, song3);
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
    try (Graph graph = GratefulDead.newGraph()) {
      Node song1 = graph.addNode(Song.label);
      Node song2 = graph.addNode(Song.label);
      song1.addEdge(FollowedBy.LABEL, song2);
      song1.addEdge(FollowedBy.LABEL, song2);
      assertNodeCount(2, graph);
      assertEdgeCount(2, graph);

      song2.remove();
      assertNodeCount(1, graph);
      assertEdgeCount(0, graph);
    }
  }

  @Test
  public void nodeRemove6() {
    try (Graph graph = GratefulDead.newGraph()) {
      Node song1 = graph.addNode(Song.label);
      Node song2 = graph.addNode(Song.label);
      Node song3 = graph.addNode(Song.label);
      song1.addEdge(FollowedBy.LABEL, song2);
      song1.addEdge(FollowedBy.LABEL, song2);
      song2.addEdge(FollowedBy.LABEL, song3);
      song2.addEdge(FollowedBy.LABEL, song3);
      song3.addEdge(FollowedBy.LABEL, song1);
      song3.addEdge(FollowedBy.LABEL, song1);

      song2.remove();
      assertNodeCount(2, graph);
      assertEdgeCount(2, graph);

      assertSize(2, graph.nodes(Song.label));

      AtomicInteger followedByEdgeCount = new AtomicInteger();
      graph.edges(FollowedBy.LABEL).forEachRemaining(edge -> followedByEdgeCount.getAndIncrement());
      assertEquals(2, followedByEdgeCount.get());
    }
  }

  @Test
  public void removeNodeProperty() {
    Graph graph = SimpleDomain.newGraph();
    final String testValue = "a string value";

    Node node = graph.addNode(TestNode.LABEL, TestNode.STRING_PROPERTY, testValue);
    assertEquals(testValue, node.property(TestNode.STRING_PROPERTY));
    assertEquals(testValue, ((TestNode)node).stringProperty());

    node.removeProperty(TestNode.STRING_PROPERTY);
    assertEquals(Optional.empty(), node.propertyOption(TestNode.STRING_PROPERTY));
    assertNull(node.property(TestNode.STRING_PROPERTY));
    assertNull(((TestNode)node).stringProperty());
  }

  @Test
  public void removeEdgeProperty() {
    Graph graph = SimpleDomain.newGraph();
    final Long testValue = 99L;

    Node n0 = graph.addNode(TestNode.LABEL);
    Node n1 = graph.addNode(TestNode.LABEL);
    Edge e0 = n0.addEdge(TestEdge.LABEL, n1, TestEdge.LONG_PROPERTY, testValue);

    { // property value should be set, no matter how we access the edge
      Edge e0FromOut = n0.outE().next();
      Edge e0FromIn = n1.inE().next();

      assertEquals(testValue, e0.property(TestEdge.LONG_PROPERTY));
      assertEquals(testValue, e0FromOut.property(TestEdge.LONG_PROPERTY));
      assertEquals(testValue, e0FromIn.property(TestEdge.LONG_PROPERTY));
    }

    {
      e0.removeProperty(TestEdge.LONG_PROPERTY);
      Edge e0FromOut = n0.outE().next();
      Edge e0FromIn = n1.inE().next();

      assertNull(e0.property(TestEdge.LONG_PROPERTY));
      assertNull(e0FromOut.property(TestEdge.LONG_PROPERTY));
      assertNull(e0FromIn.property(TestEdge.LONG_PROPERTY));
    }
  }

  @Test
  public void shouldAllowToSpecifyIds() {
    try(Graph graph = GratefulDead.newGraph()) {
      Node n10 = graph.addNode(10l, Song.label, Song.NAME, "Song 10");
      Node n20 = graph.addNode(20l, Song.label, Song.NAME, "Song 20");
      n10.addEdge(FollowedBy.LABEL, n20, FollowedBy.WEIGHT, 5);

      assertEquals(5, (int) graph.node(10).outE(FollowedBy.LABEL).next().property(FollowedBy.WEIGHT));
      assertEquals(5, (int) graph.node(20).inE(FollowedBy.LABEL).next().property(FollowedBy.WEIGHT));
    }
  }

  @Test
  public void shouldReturnElementRefs() {
    try (Graph graph = GratefulDead.newGraph()) {
      Node n0 = graph.addNode(Song.label, Song.NAME, "Song 1");
      Node n2 = graph.addNode(Song.label, Song.NAME, "Song 2");
      Edge e4 = n0.addEdge(FollowedBy.LABEL, n2);
      assertTrue(n0 instanceof NodeRef);
      assertTrue(n0.out().next() instanceof NodeRef);
    }
  }

  @Test
  public void defaultPropertyValues() {
    try (Graph graph = GratefulDead.newGraph()) {
      Node n0 = graph.addNode(Song.label, Song.NAME, "Song 1");
      Node n2 = graph.addNode(Song.label, Song.NAME, "Song 2");
      Edge e4 = n0.addEdge(FollowedBy.LABEL, n2, FollowedBy.WEIGHT, 3);

      {
        String prop0 = n0.property(Song.NAME_KEY);
        String prop1 = (String) n0.property(Song.NAME);
        String prop2 = n0.property(new PropertyKey("doesnt exist"), "default value 1");
        String prop3 = n0.property("doesnt exist", "default value 1");
        assertEquals("default value 1", prop2);
        assertEquals("default value 1", prop3);
      }

      {
        Integer prop0 = e4.property(FollowedBy.WEIGHT_KEY);
        Integer prop1 = (Integer) e4.property(FollowedBy.WEIGHT);
        Integer prop2 = n0.property(new PropertyKey<>("doesnt exist"), 99);
        Integer prop3 = n0.property("doesnt exist", 99);
        assertEquals(new Integer(99), prop2);
        assertEquals(new Integer(99), prop3);
      }
    }
  }

  private void assertNodeCount(int expected, Graph graph) {
    assertEquals("node count different to expected", expected, graph.nodeCount());
  }

  private void assertEdgeCount(int expected, Graph graph) {
    assertEquals("edge count different to expected", expected, graph.edgeCount());
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
